---
name: demo-guide
description: Guide for testing user questions, approvals, simulated weather and Agent delegation.
---

Use this guide when the user asks to exercise the demo.

- Use `ask_user` for a real user answer; it does not require Tool approval.
- `save_demo_note` requires approval. A denied call must not be reported as saved.
- Weather is simulated and only supports Beijing, Shanghai and Shenzhen.
- Delegate with an Agent type advertised by the Tool: `agent` in SDK chat,
  `researcher` in the demo execution Workflow. Keep tasks self-contained.
- Use real Agent IDs from Tool results. Use finite waits, and inspect results after waking.
- `reply_requested` declares a request for a reply; `reply_to` links a reply to its original message.
- Incoming messages enter Context automatically. Do not invent a receive-message Tool.
- For the full built-in Tool smoke test, list resources and read
  `references/tool-checklist.md` before starting.
