# Core Workflow project

Copy this directory into a new workspace and use Python 3.12 or later.
Install AutoAgent V2 with the `cli` extra. Run these commands from the
copied directory.

```bash
autoagent project check
autoagent workflow compile expense-approval
autoagent eval check expense-approval
autoagent eval run expense-approval
```

Both Cases should pass. The Eval checks the actual approval request, the
selected route, the business receipt, and whether the Workflow recorded
the expense in Context.

To drive the approval manually, run:

```bash
autoagent invoke expense-approval --entry-node submit \
  --input-json '{"expense_id":"E-3","amount_cents":2500}' --wait boundary --format json
autoagent invocation respond INVOCATION_ID --wait-id WAIT_ID \
  --input-json '{"approved":true}' --wait terminal --format json
autoagent invocation value INVOCATION_ID --field output --format json
```

Use the Invocation and Wait IDs returned by the first command. The default
local SQLite database preserves the approval across CLI calls. Set
`AUTOAGENT_DATABASE_URL` to point all three commands at another database.
