# Diagnose a live-registered Workflow

Agent App's `register_workflow` Tool publishes Workspace source into its current
Host and records the import entry in that Host's project TOML. The source stays
in its original directory; it is not copied into the execution database.

## Match source, registration and execution

Record the Workflow and Revision IDs returned by registration, then match them
to the Invocation being investigated. Editing a Python file alone does not
update an already running Host. A previously started Invocation or a deliberately
selected older Revision can correctly continue using its old implementation.

When a requested fix is authorized, change the responsible Workspace source,
register the same source directory and entrypoint again, then verify a new
Invocation through the current Server or Workflow UI. Do not use recovery of an
old Invocation as proof that the new source was loaded. Static checks validate
disk source but do not register that source into a running Server.

## Narrow a registration failure

- Import failure: check `source_dir`, the `module:attribute` entrypoint, imports
  within that source directory and the installed dependency environment.
- Compiler diagnostics: use the returned Node, Edge and contract locations to
  correct the candidate before registering again.
- Source conflict: an existing Workflow ID belongs to another source or startup
  configuration. Confirm the intended source and ID; do not overwrite another
  Workflow to make registration succeed.
- Changed source during approval: the previous authorization no longer covers
  the current files. Register the intended current candidate for approval.
- Startup extension rejection: compose its resource lifespans or Agent SDK
  runtime extension through project startup and restart the Server.

After successful registration, inspect the returned Revision through the same
Server API used for execution. The Workflow UI refreshes the current catalog;
an execution view, historical Revision or comparison stays on its selected
definition. Reopen the current definition when comparing it with edited source.

A restart imports the current source named by TOML. Relative `source_dir`
entries resolve beside that manifest. A restarted Host cannot recover a removed
old implementation from a source snapshot because none is retained. Distinguish
this from missing execution evidence: historical records can remain inspectable
even when their old code is no longer executable.
