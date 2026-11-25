"""Live registrations backed exclusively by the project's existing TOML and code."""

import asyncio
import json
import os
from pathlib import Path
from uuid import uuid4

from .models import HostError, HostRunConflict, ProjectError
from .project import Project, MANIFEST_FILENAME, validate_entrypoint


def _atomic_write(path, data):
    temporary = path.with_name("." + path.name + "." + uuid4().hex + ".tmp")
    try:
        with temporary.open("xb") as stream:
            if path.exists():
                mode = path.stat().st_mode & 0o777
                if hasattr(os, "fchmod"):
                    os.fchmod(stream.fileno(), mode)
                else:
                    temporary.chmod(mode)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _manifest(host, source, entrypoint):
    """Preserve comments, inline tables, project settings and discovered entries."""
    import tomlkit
    path = host._project.manifest_path if host._project and host._project.manifest_path else host.root / MANIFEST_FILENAME
    before = path.read_bytes() if path.exists() else None
    if before is not None:
        Project.find(path)  # Reject invalid external edits before any publication.
    document = tomlkit.parse(before.decode("utf-8")) if before is not None else tomlkit.document()
    if "schema_version" not in document:
        document["schema_version"] = 1
    if document["schema_version"] != 1:
        raise ProjectError("Project schema_version must be 1.")
    rows = document.get("workflows")
    if rows is None:
        rows = tomlkit.aot()
        # Introducing an explicit list must not hide previous decorator discovery.
        for root, value in dict.fromkeys(host._registered_workflow_sources.values()):
            row = tomlkit.table()
            row["entrypoint"] = value
            if root != host.root:
                row["source_dir"] = str(root)
            rows.append(row)
        document["workflows"] = rows
    matched = []
    for row in rows:
        if not isinstance(row, dict) and not hasattr(row, "get"):
            raise ProjectError("Workflow entries must be tables.")
        root = Path(str(row.get("source_dir", "."))).expanduser()
        root = (root if root.is_absolute() else path.parent / root).resolve()
        if root == source and row.get("entrypoint") == entrypoint:
            matched.append(row)
    if len(matched) > 1:
        raise ProjectError("Duplicate Workflow entrypoints.")
    if matched:
        matched[0]["source_dir"] = "." if source == path.parent else str(source)
    else:
        row = tomlkit.inline_table() if isinstance(rows, tomlkit.items.Array) else tomlkit.table()
        row["entrypoint"] = entrypoint
        row["source_dir"] = "." if source == path.parent else str(source)
        rows.append(row)
    return path, before, tomlkit.dumps(document).encode("utf-8")


class WorkflowRegistration:
    def _catalog_head(self):
        return {"version": self._catalog_epoch + ":" + str(self._catalog_revision),
            "workflows": dict(self._runtime.workflow_ids) if self._runtime else {}}

    async def workflow_catalog_head(self):
        return await self._perform(lambda: self._catalog_read())

    async def _catalog_read(self):
        return self._catalog_head()

    async def wait_workflow_catalog(self, version, *, timeout=15):
        """Coalesced in-memory notification; reconnects always reread the current catalog."""
        self._check_loop()
        if self._catalog_head()["version"] == version and self._state == "ready":
            event = self._catalog_changed
            try:
                await asyncio.wait_for(event.wait(), timeout)
            except TimeoutError:
                pass
        return self._catalog_head() if self._state == "ready" else None

    async def register_workflow(self, *, source_dir, entrypoint, expected_source_digest=None):
        """Register current source into this Host and its TOML without running it."""
        if asyncio.get_running_loop() is not self._loop:
            return await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(
                self.register_workflow(source_dir=source_dir, entrypoint=entrypoint, expected_source_digest=expected_source_digest), self._loop))
        source = Path(source_dir).expanduser().resolve()
        entrypoint = validate_entrypoint(entrypoint)
        async def register():
            async with self._workflow_registration_lock:
                from .source_loading import resolve_source_entrypoint
                workflow = await asyncio.to_thread(lambda: resolve_source_entrypoint(entrypoint, source, expected_digest=expected_source_digest)())
                runtime = self._runtime
                async def stage():
                    return runtime.stage_workflow(workflow)
                ir, closure, definitions = await self.runtime_services.run(stage)
                owner = (source, entrypoint)
                for value in closure.values():
                    previous = runtime.workflow_ids.get(value.workflow_id)
                    if previous and (value.workflow_id == ir.workflow_id or previous != value.workflow_revision_id):
                        if self._registered_workflow_sources.get(value.workflow_id) != owner:
                            raise HostRunConflict("workflow_source_conflict", f"Workflow {value.workflow_id!r} is already owned by another source or by Host startup configuration.")
                manifest, before, after = await asyncio.to_thread(_manifest, self, source, entrypoint)
                changed = runtime.workflow_ids.get(ir.workflow_id) != ir.workflow_revision_id
                if changed:
                    await runtime.persist_registration(closure, definitions)
                # Check again after awaits: never overwrite an intervening external edit.
                current = manifest.read_bytes() if manifest.exists() else None
                if current != before:
                    raise HostRunConflict("manifest_changed", "Project TOML changed during registration; retry with its current contents.")
                if after != before:
                    await asyncio.to_thread(_atomic_write, manifest, after)
                try:
                    if changed:
                        await runtime.publish_registration(workflow, closure, definitions)
                except BaseException:
                    if (manifest.read_bytes() if manifest.exists() else None) == after and after != before:
                        if before is None:
                            manifest.unlink(missing_ok=True)
                        else:
                            await asyncio.to_thread(_atomic_write, manifest, before)
                    raise
                for value in closure.values():
                    self._registered_workflow_sources.setdefault(value.workflow_id, owner)
                if self._project is not None:
                    self._project = Project.find(manifest)
                if changed:
                    self._catalog_revision += 1
                    event, self._catalog_changed = self._catalog_changed, asyncio.Event()
                    event.set()
                return {"status": "registered", "workflow_id": ir.workflow_id, "revision_id": ir.workflow_revision_id,
                    "changed": changed, "manifest": str(manifest), "entries": [
                        {"node_id": node_id, "input_schema": json.loads(ir.node(node_id).input_contract.schema)
                            if ir.node(node_id).input_contract else None} for node_id in ir.entry_node_ids]}
        return await self._perform(register, control=True)
