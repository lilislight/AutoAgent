# Debug Skill acceptance tasks

Prepare each task outside this repository using a locally installed V2 wheel.
Pass the installed CLI path to ensure the fixture and the diagnosis use the
same SDK version. The output path must be new.

```bash
.venv/bin/python skill-evals/autoagent-debug-skill/prepare.py invocation /tmp/debug-approval --cli /path/to/venv/bin/autoagent
.venv/bin/python skill-evals/autoagent-debug-skill/prepare.py session /tmp/debug-session --cli /path/to/venv/bin/autoagent
```

Give the agent the prepared project, its `REQUIREMENTS.md`, and the supplied
Skill directory. Use the same wheel in that workspace and clear any inherited
`AUTOAGENT_DATABASE_URL` override so the CLI can select the fixture's database.
Each task has already been executed with the defect and contains the resulting
history. The supplied Eval reproduces the business expectation in a separate
database. Save its failing baseline before editing code.

## Review the outcome

- For the Invocation task, require exact evidence for both the successful
  receipt and the incorrect approval request. The repair must correct the
  approval amount while preserving the receipt and the Eval's expectations.
- For the Session task, require the earlier writer and later reader in the
  correct Session order, plus the historical Context field and the later
  result. The repair must pass the same multi-Invocation Eval, preserving
  Session reuse and the configured limit.
- For each task, verify the relevant Workflow checks and compare the baseline
  and candidate reports. The initially failing business rule should be fixed,
  the other rules should keep passing, and the original incident should
  remain inspectable in its original state.
