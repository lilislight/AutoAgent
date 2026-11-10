# Tool smoke test

Use only a new test directory selected by the user. Do not edit project source,
credentials, Git branches or existing files. Read files before editing/moving
or deleting them. Remove only files created by this test.

- Skills: `skill_load`, `skill_list_resources`, `skill_read_resource`.
- Files: `list_files`, `search_files`, `read_file`, `write_file`, `edit_file`,
  `manage_files` (stat, mkdir, copy, move, delete).
- CLI: `execute_command`, `manage_process` (list, read, write, terminate).
- Human interaction: `ask_user`.
- Collaboration: `spawn_agent`, `run_agent`, `get_agents`, `send_message`,
  `wait_for`, `cancel_agent`.
- Demo extras: `add_numbers`, `get_demo_weather`, `save_demo_note`.

CLI probes run from the project Workspace using
`python3 -u -m agent_demo.cli_probe ACTION`. Available actions: `info`, `count`,
`echo`, `sleep`, `fail`, `approval`, `deny`. Only the configured probe invocation
is allowed; unrelated Python commands still need approval. The `approval`
probe must ask; the `deny` probe must never execute. `fail` intentionally exits
with code 3. A timeout is an expected result for the timeout test, not a reason
to repeat the command.

Choose the actual Agent type advertised in the Tool schema: the SDK chat uses
`agent`, while the demo execution Workflow uses `researcher`. Child Agents must
inspect already received messages before waiting, use finite waits, and stop
after completing their assigned test. Never invent Agent IDs or message IDs.
