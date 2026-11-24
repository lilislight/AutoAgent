# Offline Agent Workflow project

Copy this directory into a new workspace and use Python 3.12 or later.
Install AutoAgent V2 with the `cli` and `agent` extras. Run these commands
from the copied directory.

```bash
autoagent project check
autoagent workflow compile release-note-assistant --node-id start
autoagent eval check release-note-approval
autoagent eval run release-note-approval
```

The project registers an offline Provider in its lifespan. It requests
a Tool call, waits for an explicit approval, and consumes the Tool
result. The two Eval Cases assert that the in-memory note log stays
empty before approval and after refusal, and contains one note after
approval. The Provider's scripted replies make these checks repeatable.

To observe the pending approval from the CLI:

```bash
autoagent invoke release-note-assistant --entry-node start \
  --input-json '{"message":"Record the release note."}' --wait boundary --format json
```

The Eval's `approve_tool_call` shows how to turn its pending
`AgentApprovalRequest` into the JSON accepted by
`autoagent invocation respond`.
