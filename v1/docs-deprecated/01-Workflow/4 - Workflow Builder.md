# Workflow Builder

This document defines Workflow authoring frontends and how they produce the
canonical Workflow object.

Builder/loader output is Workflow. Compiler output is Workflow IR.

## Frontends

| Frontend | Purpose |
| --- | --- |
| Python API | Object-based, type-friendly authoring. |
| YAML | Declarative, human-editable workflow specification. |
| JSON | API-friendly serialized workflow specification. |
| Future UI | Visual authoring that emits JSON/YAML or canonical specs. |
| Optimizer Patch | Structured changes that produce a new Workflow version. |

## Python API

Python authoring may pass functions directly, or use string references for
system/external capabilities.

```python
from autoagent import Edge, Workflow


def fetch_issue(ticket_id: str) -> dict:
    ...


def classify_issue(issue: dict) -> dict:
    ...


workflow = Workflow()

fetch = workflow.add_node(
    fetch_issue,
    entry=True,
)

classify = workflow.add_node(classify_issue)

workflow.add_edge(Edge(from_node=fetch, to_node=classify))
```

The Python API stores direct functions as node capabilities. String references
remain useful for system capabilities, YAML/JSON loading, and UI-authored
workflows.

## Workflow Visualization

Workflow should provide an inspection utility for checking graph structure before
execution.

Use Mermaid when embedding the graph in Markdown or other documentation:

```python
print(workflow.to_mermaid())
```

Generate a standalone HTML/SVG preview for local inspection:

```python
preview_path = workflow.preview("workflow_preview.html")
```

When the Workflow uses `CapabilityRef` or `OperatorRef`, render through the App
so preview compilation can see its registries:

```python
preview_path = app.preview(workflow, "workflow_preview.html")
```

The preview runs Compiler validation without executing the Workflow. Valid
edges are gray, warning edges are amber, and invalid edges are red with their
diagnostics shown below the graph. This static authoring preview is separate
from the future Runtime observability graph.

## YAML

YAML uses string capability references because it cannot carry Python objects.
The compiler resolves those references through the configured execution
environment when needed.

```yaml
id: github_issue_triage
version: 1.0.0
name: GitHub Issue Triage

nodes:
  - id: fetch_issue
    capability: operator:github.fetch_issue
    entry: true

  - id: classify_issue
    capability: operator:llm.classify_issue

edges:
  - id: fetch_to_classify
    from: fetch_issue
    to: classify_issue
```

Example with condition and policy:

```yaml
id: coding_agent
version: 1.0.0

nodes:
  - id: parse_request
    capability: operator:request.parse
    entry: true

  - id: fetch_jira
    capability: operator:jira.fetch_ticket
    policy:
      retry:
        max_attempts: 3
      timeout:
        duration: 30s

  - id: ask_user_for_requirements
    capability: system:wait_human_input

edges:
  - id: parse_to_jira
    from: parse_request
    to: fetch_jira

  - id: jira_missing_to_user
    from: fetch_jira
    to: ask_user_for_requirements
    condition: nodes.fetch_jira.output.requirements == null
```

## JSON

JSON follows the same schema as YAML.

```json
{
  "id": "github_issue_triage",
  "version": "1.0.0",
  "nodes": [
    {"id": "fetch_issue", "capability": "operator:github.fetch_issue", "entry": true},
    {"id": "classify_issue", "capability": "operator:llm.classify_issue"}
  ],
  "edges": [
    {"id": "fetch_to_classify", "from": "fetch_issue", "to": "classify_issue"}
  ]
}
```

## Loader Pipeline

YAML/JSON loading pipeline:

1. Parse source into raw dictionaries.
2. Validate required fields.
3. Normalize capability strings.
4. Create Node and Edge objects.
5. Apply simple defaults.
6. Validate basic structure.
7. Return Workflow.

Pseudo-code:

```python
def load_workflow_from_yaml(path: str) -> Workflow:
    raw = yaml.safe_load(open(path))
    return WorkflowSpecLoader().load(raw)


class WorkflowSpecLoader:
    def load(self, raw: dict) -> Workflow:
        nodes = [self.load_node(item) for item in raw.get("nodes", [])]
        edges = [self.load_edge(item) for item in raw.get("edges", [])]
        workflow = Workflow(
            id=raw["id"],
            version=str(raw["version"]),
            name=raw.get("name"),
            description=raw.get("description"),
            policy=self.load_policy(raw.get("policy")),
            metadata=raw.get("metadata", {}),
            nodes=nodes,
            edges=edges,
        )
        self.validate_structure(workflow)
        return workflow
```

## Validation Boundary

Loader validation:

- Workflow id and version exist.
- Node ids are unique.
- Edge references point to existing nodes.
- Capability references are syntactically valid.
- Required node and edge fields exist.

Compiler validation:

- Entry and exit inference.
- Graph analysis.
- Capability resolution through registries.
- Input mapping inference.
- Schema compatibility.
- Join/routing/policy semantics.
- Workflow IR generation.
