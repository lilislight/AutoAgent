# Agent Skills Example

Run from the repository root. Examples use public `autoagent.agent` APIs and
application-owned calls; they do not build a Workflow.

## Install and Run

```bash
uv pip install --python .venv/bin/python -e '.[skills]'
.venv/bin/python -m examples.agent_skills_demo.run
.venv/bin/python -m examples.agent_skills_demo.run --activation tools --verbose
.venv/bin/python -m examples.agent_skills_demo.capabilities
```

The first example explicitly loads instructions, rules and a template, then
supplies inventory data to a deterministic offline Provider. Tool activation
starts with a catalog, requests Skill content through Tools, and returns a
report. The bounded model-call loop belongs to the application. Offline mode
exercises content/Tool wiring; it does not evaluate model instruction following.

Expected findings: A has four sellable days versus seven days for replenishment
and needs at least 15 additional units; B is already out of stock; C lacks daily
sales data and cannot receive a sellout forecast.

The capabilities example additionally demonstrates skipped-file diagnostics,
manual-only Skills, resource pagination, text continuation, Local/Memory/Package
sources and a real execution of the explicitly bound calculator script.

## Live Provider

```bash
uv pip install --python .venv/bin/python -e '.[skills,agent]'
```

Set `OPENAI_API_KEY` and `LLM_MODEL` in the shell, then run:

```bash
.venv/bin/python -m examples.agent_skills_demo.run --mode live --activation tools --verbose
.venv/bin/python -m examples.agent_skills_demo.run --mode live --protocol chat
```

`--model` overrides `LLM_MODEL`. `OPENAI_BASE_URL` optionally selects an
OpenAI-compatible endpoint. Live mode makes real API requests. Model tool
selection varies; `--max-rounds` bounds the application's model calls.

## Discovery and Diagnostics

```python
from autoagent.agent import SkillSet

skills = SkillSet.discover(["./skills"], errors="skip")
for diagnostic in skills.diagnostics:
    print(diagnostic.location, diagnostic.code, diagnostic.field, diagnostic.line)
```

Default `errors="raise"` fails immediately. Skip mode retains valid Skills and
records malformed/missing sources and duplicates. The first valid duplicate
wins; collisions remain visible in diagnostics. Scan limits stop discovery
rather than becoming unbounded in skip mode. Missing YAML dependencies still
fail explicitly. Diagnostics identify fields and YAML lines without printing
source values.

Discovery scans only direct child directories of explicit roots. Rediscovery
creates a new collection. Metadata edits require rediscovery for all document,
resource, text and script access. Previously loaded content remains detached.

## Manual and Model Access

```yaml
---
name: manual-report
description: Guidance selected explicitly by the application.
disable-model-invocation: true
---
```

`skills.load("manual-report")` remains available to the application.
`catalog_message()`, `model_metadata`, `for_model()` and `as_tools()` filter it
from model-facing capabilities. Tool execution checks access again, including
source metadata changes. The flag must be a YAML boolean.

An empty or entirely manual-only collection creates neither a catalog message
nor Tools. `allowed-tools` remains a declaration; it does not grant execution
or create connections. Applications choose their ToolSet and script bindings.

## Loading and Resource Pages

```python
selected = skills.select(include=("inventory-report",))
document = selected.load("inventory-report")
page = selected.list_resources("inventory-report", prefix="references", limit=20)
if page.next_cursor:
    next_page = selected.list_resources(
        "inventory-report", prefix="references", limit=20, cursor=page.next_cursor,
    )
```

`load()` loads only metadata and instructions. Resource enumeration is a
separate operation returning `SkillResourcePage.resources`, `revision` and
`next_cursor`. Prefixes prune unrelated directories. A listing change invalidates
its cursor instead of silently skipping entries. Listing revisions describe
paths, MIME types and available sizes, not file contents. Scans remain bounded;
raise `max_scan_entries` explicitly for larger catalogs. Catalog scans omit
conventional generated directories such as `__pycache__`, `.git`,
`.pytest_cache`, `.venv` and `node_modules`.

## Text Ranges and Media

```python
chunk = selected.read_text(
    "inventory-report", "references/report-rules.md", start_line=1, max_lines=20,
)
if chunk.next_line:
    continuation = selected.read_text(
        "inventory-report", "references/report-rules.md", start_line=chunk.next_line,
    )
resource = selected.read_resource("inventory-report", "assets/report.pdf")
```

Text ranges stream complete UTF-8 lines. `max_text_lines` and `max_text_bytes`
limit returned content; `max_resource_bytes` also bounds scanning skipped bytes.
Oversized individual lines fail explicitly. Text-range revisions hash the
returned bytes; continuation reads the live source, not a frozen file version.

Full resources use existing Text/Image/Audio/Video/File content models. Provider
adapters determine which modalities their endpoints support. `SKILL.md` reads
use document validation and limits even through the resource APIs. Paths are
relative POSIX paths, with no traversal, blank components or external links.
POSIX descriptor-relative file reads reject link swaps; the portable fallback
assumes application-owned directories. Directory links are not traversed.

Defaults: 256 KiB per SKILL.md, 8 MiB per full resource or text scan, 256 resources
per page, 2000 scan entries, eight directory levels, 200 text lines and 64 KiB
per text range. Over-limit requests fail rather than silently truncating data.

## Portable Skill Sources

```python
from autoagent.agent import (
    Skill, SkillSet, LocalSkillSource, MemorySkillSource, PackageSkillSource,
)

local = LocalSkillSource("./skills/inventory-report")
memory = MemorySkillSource("my-skill", {
    "SKILL.md": "---\nname: my-skill\ndescription: Example\n---\nInstructions.",
    "references/rules.md": "Reference text.",
})
package = PackageSkillSource("my_package", "skills/bundled-skill")
loaded = Skill.from_source(memory).load()
collection = SkillSet.from_sources((local, memory, package), errors="skip")
```

Local sources constrain file access to a bound directory. Memory sources copy
uploaded or remotely fetched bytes. Package sources support normal and zipped
packages. Source names match the name inside SKILL.md.

Application adapters implement `SkillSource`: `name`, credential-free
`location`, `open_binary(path)`, `resource_info(path)` and
`iter_resources(prefix=..., limits=...)`. Binary streams are context-owned.
Sources may leave resource size unknown. These synchronous adapters run through
Tool's worker pool. Async MCP/network clients retain their own lifecycle;
prefetch their returned bytes into MemorySkillSource on the client's owning
loop. The library does not clone repositories or create remote connections.

The MCP helper performs that prefetch with per-file and total byte bounds:

```python
async with MCPClient(config, auth=auth) as client:
    source = await MemorySkillSource.from_mcp(client, "inventory-report", {
        "SKILL.md": "skill://inventory-report/SKILL.md",
        "references/rules.md": "skill://inventory-report/references/rules.md",
    })
skills = SkillSet.from_sources((source,))  # Detached from the closed MCP client.
```

Choose URIs exposed by the actual server. Each mapped URI must return one text
or base64-encoded blob content item. Failed/cancelled fetches return no partial
source. Blob MIME defaults are inferred from mapped filenames.
Explicit server MIME types take precedence; text items without MIME default to
text/plain. Memory sources also accept `mime_types={path: mime}` overrides.

## Script Runner

```python
import sys
from autoagent.agent import SkillScriptSpec, SubprocessSkillRunner, ToolSet

runner = SubprocessSkillRunner((SkillScriptSpec(
    skill_name="inventory-report",
    path="scripts/calculate.py",
    command=(sys.executable,),
    resources=(),
),), max_timeout_seconds=30, max_output_bytes=128 * 1024)
tools = ToolSet(skills.as_tools(runner=runner))
```

An injected runner adds `skill_run_script`. Without it, script files are only
readable. Bindings select exact Skill names, script paths, command prefixes and
optional companion files. Execution stages only those files into a temporary
working directory. Arguments are passed through subprocess argv; argument count,
combined stdin/argument bytes and combined stdout/stderr have limits. Nonzero exits retain their outputs as
Tool error observations. Timeout/output overflow terminates execution; cancelling
a call waits for owned subprocess and workspace cleanup. POSIX process groups
reclaim ordinary child processes as well.

The subprocess runner uses the application's OS permissions. Deploy it inside
application-selected isolation when scripts need stronger restrictions.
Configured `env` replaces the child environment; otherwise it is inherited.
Environment and runner objects are runtime resources, not checkpoint values.
Applications may inject any `SkillScriptRunner` implementation. Its result must
match the requested Skill and path.

## Tool and MCP Composition

Normal collections provide `skill_load`, `skill_list_resources`,
`skill_read_resource` and `skill_read_text`. An injected runner adds
`skill_run_script`. Tool schemas advertise only model-enabled Skill names.
Change names with `as_tools(prefix="my_skill_")` as needed.

```python
async with MCPClient(config, auth=auth) as client:
    remote = await client.tools(include=("get",), prefix="inventory_")
    tools = ToolSet((*skills.as_tools(), *remote))
```

Adapt remote names to the server. Use `tools.definitions` in LLMRequest and
`tools.ainvoke` for calls. Skill loading does not create a connection or Agent
loop. Expected Skill failures become typed error observations; unexpected
adapter/runner implementation failures use the existing Tool error boundary.
