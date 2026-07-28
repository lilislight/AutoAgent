# AutoAgent Authoring Skill Evaluations

These projects evaluate whether a Coding Agent can author a working AutoAgent
project from a product requirement. Each directory intentionally contains only
its `REQUIREMENTS.md`.

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
