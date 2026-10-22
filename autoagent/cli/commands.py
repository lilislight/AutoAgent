"""Offline project commands, using Host compilation without opening a Host."""

from pathlib import Path

from . import CliError


def inspect_project(options):
    """Resolve the existing Host project/environment rules and compile once."""
    try:
        from autoagent.host import AutoAgentHost, ProjectError
        from autoagent.host.project import Project, import_scope
    except ModuleNotFoundError as error:
        raise CliError(
            "DEPENDENCY_MISSING", "Install autoagent[cli] to inspect projects.",
            exit_code=3, details={"module": error.name},
        ) from error
    try:
        project = Project.find(getattr(options, "project", "."))
        # Console scripts omit cwd from sys.path; match `python -m autoagent`
        # while keeping the selected project's own import paths first.
        with import_scope(Path.cwd()):
            report = AutoAgentHost.check_project(
                getattr(options, "project", "."),
                env_file=getattr(options, "env_file", None),
                use_env_file=not getattr(options, "no_env_file", False),
            )
    except ProjectError as error:
        raise CliError("PROJECT_INVALID", str(error)) from error
    return project.root, report


def _workflow_record(result):
    record = result.to_diagnostic_document()
    ir = result.workflow_ir
    record.update(
        workflow_version=ir.workflow_version if ir else None,
        workflow_revision_id=ir.workflow_revision_id if ir else None,
        definition_hash=ir.definition_hash if ir else None,
    )
    return record


def run(options, root: Path, report):
    """Return one output document and exit code; never start project resources."""
    command = f"{options.group}.{options.command}"
    strict = getattr(options, "warnings_as_errors", False)
    source = {"kind": "project", "root": str(root), "name": report.name}
    if options.group == "project":
        records = [_workflow_record(result) for result in report.workflows]
        counts = {level: sum(row["summary"][level] for row in records) for level in ("error", "warning", "info")}
        ok = report.ok and not (strict and counts["warning"])
        data = {"summary": {"workflows": len(records), **counts}, "workflows": records}
    else:
        result = next((value for value in report.workflows if value.workflow_id == options.workflow_id), None)
        if result is None:
            raise CliError("WORKFLOW_NOT_FOUND", f"Workflow is not declared in this project: {options.workflow_id}")
        data = _workflow_record(result)
        ok = result.ok and not (strict and data["summary"]["warning"])
        if options.command == "compile":
            data["plan"] = None
            if result.ok:
                snapshot = result.workflow_definition_snapshot.to_record()
                definition = snapshot["definition"]
                data["definition_schema_version"] = snapshot["schema_version"]
                node_id, edge_id = options.node_id, options.edge_id
                if node_id is not None:
                    node = next((row for row in definition["nodes"] if row["id"] == node_id), None)
                    if node is None:
                        raise CliError("NODE_NOT_FOUND", f"Workflow {options.workflow_id} has no Node: {node_id}")
                    data["selection"] = {"kind": "node", "id": node_id}
                    data["plan"] = {
                        "node": node,
                        "incoming_edges": [edge for edge in definition["edges"] if edge["target"] == node_id],
                        "outgoing_edges": [edge for edge in definition["edges"] if edge["source"] == node_id],
                        "loops": [loop for loop in definition["loops"] if node_id in loop["nodes"]],
                    }
                elif edge_id is not None:
                    edge = next((row for row in definition["edges"] if row["id"] == edge_id), None)
                    if edge is None:
                        raise CliError("EDGE_NOT_FOUND", f"Workflow {options.workflow_id} has no Edge: {edge_id}")
                    data["selection"] = {"kind": "edge", "id": edge_id}
                    data["plan"] = {"edge": edge, "loops": [loop for loop in definition["loops"] if edge_id in loop["entries"] + loop["backs"] + loop["exits"]]}
                else:
                    data["plan"] = definition
    document = {"schema_version": 1, "command": command, "ok": bool(ok), "source": source, "data": data}
    document["policy"] = {"warnings_as_errors": strict}
    return document, 0 if ok else 1
