"""Versioned CLI documents and readable, colour-free terminal reports."""

import json
import os
import shlex
from pathlib import Path
import tempfile
from contextlib import contextmanager
import sys


def _json(value):
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


def _diagnostics(record):
    lines = []
    for diagnostic in record["diagnostics"]:
        subject = ".".join(str(diagnostic[key]) for key in ("object_type", "object_id", "field") if key in diagnostic)
        lines.append(f"  {diagnostic['severity'].upper()} {diagnostic['code']}" + (f" [{subject}]" if subject else ""))
        lines.append(f"    {diagnostic['message']}")
        if diagnostic.get("hint"):
            lines.append(f"    Hint: {diagnostic['hint']}")
    return lines


def _identity(record):
    return [f"WORKFLOW {record['workflow_id']}", f"VERSION {record['workflow_version'] or 'unavailable'}", f"REVISION {record['workflow_revision_id'] or 'unavailable'}"]


def _hook_label(hook):
    version = hook.get("version")
    return hook["name"] + (f"@{version}" if version is not None else "")


def render(document, format):
    if format == "json":
        return _json(document) + "\n"
    if "error" in document:
        error = document["error"]
        lines = [f"ERROR {error['code']}: {error['message']}"]
        details = error.get("details", {})
        if details.get("usage"):
            lines.append(details["usage"])
        if details.get("hint"):
            lines.append("Hint: " + details["hint"])
        errors = details.get("errors") or (details.get("detail", {}).get("errors") if isinstance(details.get("detail"), dict) else None) or []
        for item in errors:
            lines.append(f"  {'.'.join(str(part) for part in item.get('path', [])) or '$'}: {item['message']}")
        for key in ("invocation_id", "session_id", "request_id", "operation_id", "checkpoint_id"):
            if key in details:
                lines.append(f"{key.upper()} {details[key]}")
        return "\n".join(lines) + "\n"
    source, data, command = document["source"], document["data"], document["command"]
    lines = [f"SERVER {source['url']}", ""] if source["kind"] == "server" else [f"PROJECT {source['name']}", f"ROOT {source['root']}", ""]
    if command.startswith('eval.'):
        from .evaluation import render as render_evaluation
        return '\n'.join(lines + [render_evaluation(data, command)]) + '\n'
    if document.get("view") in ('durable', 'live'):
        from .history import render_query
        lines += ["VIEW " + document['view'], render_query(data)]
        return "\n".join(lines) + "\n"
    if command == "server":
        lines += ["SERVER ready", f"API {data['api_url']}"]
        if data.get("listening"):
            lines.append(f"LISTENING {data['listening']}")
        if "registered_workflows" in data:
            lines.append(f"WORKFLOWS {data['registered_workflows']}")
        if data["ui_url"]:
            lines.append(f"UI {data['ui_url']}")
        lines.append("Press Ctrl+C to stop and save Host state.")
        return "\n".join(lines) + "\n"
    if command == 'memory':
        lines.append('MONITOR ' + ('enabled' if data['enabled'] else 'disabled'))
        sample = data.get('latest')
        if sample is None:
            lines.append('Set AUTOAGENT_MEMORY_MONITOR_ENABLED=true before starting the Server.' if not data['enabled'] else 'No sample available yet.')
        else:
            def mib(value):
                return f'{value / 1024**2:.2f} MiB' if value is not None else 'unavailable'
            lines.extend([f"PID {sample['pid']}", f"RSS {mib(sample['process']['rss_bytes'])}",
                          f"PEAK RSS {mib(sample['process']['peak_rss_bytes'])}",
                          f"BUDGET {mib(sample['budget_bytes'])}", f"PRESSURE {sample['pressure']}",
                          f"SAMPLE AGE {data['sample_age_seconds']:.3f}s" + (' (stale)' if data['stale'] else '')])
            host = sample.get('host') or {}
            persistence = host.get('persistence', {})
            lines.extend([f"RESIDENT GRAPHS {host.get('resident_graphs', 'unavailable')}",
                          f"PERSISTENCE QUEUE {persistence.get('queued_count', 'unavailable')}",
                          f"PERSISTENCE RETAINED {mib(persistence.get('retained_bytes'))}"])
            estimates = host.get('residency', {})
            lines.append(f"CACHED STATE ESTIMATE {mib(estimates.get('estimated_bytes'))} (not an RSS breakdown)")
            if data.get('history'):
                lines.append(f"HISTORY {len(data['history'])} points / {data['history_count']} selected samples; {data['history_retained_samples']}/{data['history_capacity']} retained (use --format json for values)")
        return '\n'.join(lines) + '\n'
    if command == "invoke" or command.startswith("invocation."):
        for key in ("workflow_id", "revision_id", "session_id", "invocation_id", "operation_id", "request_id", "status", "control_state", "observation", "host_state"):
            if data.get(key) is not None:
                lines.append(f"{key.upper()} {data[key]}")
        for wait in data.get("waits", []):
            lines.append(f"WAIT {wait['wait_id']}  kind={wait['kind']}  invocation={wait['invocation_id']}")
        if data.get("waits_truncated"):
            lines.append("WAITS truncated: showing the first 100 pending Waits.")
        if data.get("error_type"):
            lines.append(f"ERROR {data['error_type']} {data.get('error_code') or ''}".rstrip())
        if data.get("error_message"):
            lines.append(data["error_message"])
        for node in data.get("failed_nodes", []):
            lines.append(f"FAILED NODE {node['node_id']}  occurrence={node['occurrence_id']}")
        if data.get("elapsed_seconds") is not None:
            lines.append(f"ELAPSED {data['elapsed_seconds']:.3f}s")
        iid = data.get("invocation_id")
        if iid:
            prefix = ("autoagent --server " + shlex.quote(source["url"]) if source["kind"] == "server"
                      else "autoagent --project " + shlex.quote(source["root"]))
            if data.get("status") == "completed":
                lines.append(f"View output: {prefix} invocation value {shlex.quote(iid)} --field output")
            elif data.get("status") == "failed":
                lines.append(f"Inspect failure: {prefix} invocation diagnose {shlex.quote(iid)}")
                lines.append(f"Read error: {prefix} invocation value {shlex.quote(iid)} --field error")
            elif data.get("waits"):
                lines.append(f"Respond: {prefix} invocation respond {shlex.quote(iid)} --wait-id WAIT_ID --input-json JSON")
        return "\n".join(lines) + "\n"
    strict = document.get("policy", {}).get("warnings_as_errors", False)
    if strict:
        lines.append("POLICY warnings-as-errors")
    if command == "project.check":
        for record in data["workflows"]:
            passed = record['ok'] and not (strict and record['summary']['warning'])
            lines.append(f"WORKFLOW {record['workflow_id']}  {'passed' if passed else 'failed'}")
            lines.extend(_diagnostics(record))
        counts = data["summary"]
        lines += ["", f"CHECK {'passed' if document['ok'] else 'failed'}: {counts['workflows']} Workflows, {counts['error']} errors, {counts['warning']} warnings"]
    else:
        lines += _identity(data) + _diagnostics(data)
        lines.append(f"CHECK {'passed' if document['ok'] else 'failed'}")
        if command == "workflow.compile" and data["plan"] is not None:
            plan = data["plan"]
            lines.append("")
            if "selection" in data:
                lines.append(_json(plan))
            else:
                lines += [f"FAILURE_MODE {plan['failure_mode']}", f"ENTRIES {', '.join(plan['entry_node_ids'])}", f"EXITS {', '.join(plan['exit_node_ids'])}"]
                for node in plan["nodes"]:
                    executable = node["executable"]
                    kind, identity = executable["kind"], executable["id"]
                    label = identity if identity.startswith(kind + ":") else f"{kind}:{identity}"
                    lines.append(f"NODE {node['id']}  {label}")
                    lines.append(f"  RECOVERY {node['recovery']['mode']}  RESOURCE {json.dumps(node['resource'], sort_keys=True)}")
                    if node["map"]:
                        lines.append(f"  MAP max_parallelism={node['map']['max_parallelism']}")
                        if node['map']['aggregate']:
                            lines.append(f"  AGGREGATE {_hook_label(node['map']['aggregate'])}")
                    for key in ("input_mapping", "output_binding"):
                        if node[key] is not None:
                            lines.append(f"  {key.upper()} {_hook_label(node[key])}")
                    if node["stream"]:
                        lines.append(f"  STREAM {node['stream']['reducer']}")
                    for key in ("retry", "timeout"):
                        if node.get(key) is not None:
                            lines.append(f"  {key.upper()} {json.dumps(node[key], ensure_ascii=False, sort_keys=True)}")
                for edge in plan["edges"]:
                    condition = f" condition={edge['condition']['name']}" if edge["condition"] else ""
                    lines.append(f"EDGE {edge['id']}  {edge['source']} -> {edge['target']}  on={edge['on']}{condition}")
                for loop in plan["loops"]:
                    lines.append(f"LOOP {loop['id']}  header={loop['header']}  back={','.join(loop['backs'])}")
                for endpoint in plan["signal_endpoints"]:
                    lines.append(f"SIGNAL {endpoint['name']}")
                lines.append("Use --node-id / --edge-id for details, or --format json for the full compiled plan.")
    return "\n".join(lines) + "\n"


def report_path(output, root):
    path = Path(output).expanduser()
    if not path.is_absolute():
        path = root / path
    return path


@contextmanager
def report_file(path):
    """Publish complete reports atomically; failed exports leave the old file intact."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as stream:
            temporary = stream.name
            yield stream
        os.replace(temporary, path)
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)


def write_report(text, output, root, stdout, stderr):
    if output is None or output == "-":
        stdout.write(text)
        stdout.flush()
        return
    path = report_path(output, root)
    with report_file(path) as stream:
        stream.write(text)
    stderr.write(f"Report written to {path}\n")


def failure_root(options, root):
    if root is not None:
        return root
    if getattr(options, '_root', None) is not None:
        return options._root
    if options is not None and not getattr(options, 'server_url', None):
        try:
            from autoagent.host.project import Project
            return Project.find(getattr(options, 'project', '.')).root
        except Exception:
            pass
    return Path.cwd()


async def export_pages(pages, output, root, format, stderr=None):
    """Stream paginated metadata into one report using memory proportional to page size."""
    from . import CliError
    path = report_path(output, root)
    first, footer, count, page_count, first_frontier = True, {}, 0, 0, None
    try:
        with report_file(path) as stream:
            async for document in pages:
                data = document['data']
                if first:
                    first_frontier = data.get('frontier')
                    footer = {key: value for key, value in data.items() if key not in ('items', 'next_cursor', 'returned_count', 'frontier')}
                    if format == 'json':
                        header = {key: value for key, value in document.items() if key != 'data'}
                        stream.write(json.dumps(header, ensure_ascii=False)[:-1] + ', "data": {"items": [\n')
                if format == 'json':
                    for row in data['items']:
                        if count:
                            stream.write(',\n')
                        stream.write(json.dumps(row, ensure_ascii=False))
                        count += 1
                else:
                    data = dict(data, next_cursor=None)
                    if first:
                        stream.write(render(dict(document, data=data), 'text'))
                    else:
                        from .history import render_query
                        stream.write(render_query(data) + '\n')
                    count += len(data['items'])
                page_count += 1
                footer['frontier'] = data.get('frontier')
                first = False
            if first:
                raise CliError('INVALID_REPORT', 'The export produced no query response.', exit_code=3)
            footer.update(next_cursor=None, returned_count=count, pages=page_count, first_frontier=first_frontier)
            if format == 'json':
                stream.write('\n], ' + json.dumps(footer, ensure_ascii=False)[1:] + '}\n')
            else:
                stream.write(f'EXPORTED {count} records in {page_count} pages\n')
    except OSError as error:
        raise CliError('OUTPUT_WRITE_FAILED', str(error), exit_code=3) from error
    (stderr or sys.stderr).write(f'Report written to {path}\n')
