# Register Workspace code into the current Host

Use this path when developing a Workflow from Agent App. The authoring and
debugging Skills are bundled with the installed SDK. The default Workspace is
the Server's project root; use the conversation's actual bound Workspace when
it differs. Keep new source and project-specific artifacts there. A conversation
without a Workspace needs a new conversation bound to the intended directory
before using the registration Tool.

## Write and register

Create ordinary importable Python modules, preferably in a focused directory
such as `workflows/`. Export a Workflow object or a zero-argument synchronous
factory. Use module-level `TypedDict` or Pydantic models for contracts. The
factory constructs the graph; it does not invoke it.

For `workflows/greeting_demo.py` exporting `build_workflow`, call:

```json
{
  "source_dir": "workflows",
  "entrypoint": "greeting_demo:build_workflow"
}
```

Send these arguments to `register_workflow`. `source_dir` is relative to the
conversation's Workspace working directory. `entrypoint` is importable relative
to that source directory and uses `module:attribute` syntax.

The Tool imports and validates the current source, registers it into the running
Host, and updates that Host's actual project TOML while retaining unrelated
configuration. Do not separately add the same entry to TOML as part of this
operation. Registration does not execute the Workflow. Report the returned
Workflow ID, Revision ID, Entry Node IDs and input schema.

The Tool uses ordinary approval policy and defaults to Ask. Source changes
invalidate pending approval. If registration fails, use its import or compiler
diagnostics to fix the responsible source, then register that candidate.

## Execute and inspect

The Workflow appears in the existing Workflow UI without restarting the Server.
Use the UI or the installed CLI connected to that same Server:

```bash
autoagent --server API_BASE invoke WORKFLOW_ID --entry-node ENTRY_NODE --input-json '{"name":"AutoAgent"}' --wait terminal
autoagent --server API_BASE invocation diagnose INVOCATION_ID
```

Take `API_BASE`, including its API prefix, from the running Server's connection
or configuration. Use the returned IDs and the Workflow's real input schema;
the input above is only a greeting example. Do not open another execution Host
against the Server's database. Local static checks remain useful and do not
publish code into an already running Host.

## Update and restart

Source files remain in their original directory. Registration adds an optional
`source_dir` to the Host's `[[workflows]]` entry. Relative TOML source directories
resolve beside that manifest, independent of process working directory. The
registration Tool handles this path when the Workspace differs from the Host
project root.

Editing source alone does not replace the loaded implementation. Register the
same entrypoint again to publish the current code. Existing executions and
explicitly selected old Revisions retain their previously loaded code within
that Host process. Start a new Invocation to verify the new Revision.

A Server restart imports the current disk source from TOML. There is no saved
source snapshot, so a restart cannot reconstruct an old implementation that
has been removed or changed. Keep registered source importable.

Live registration supports ordinary Workflows using existing Host capabilities.
New startup resource lifespans and Agent SDK runtime extensions must be composed
through the project startup configuration and loaded by restarting the Server.
