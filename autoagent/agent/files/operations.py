"""File-operation facts advertised to permission editors, never to the model.

resource is the resolved absolute target. attributes.path is Workspace-relative
inside the root and absolute outside it; both use POSIX separators. Filesystem
identity/anchor attributes are execution checks, not permission defaults.
"""


FILE_OPERATION_SCHEMA = {
    "type": "object",
    "required": ["action", "resource", "attributes"],
    "properties": {
        "action": {"type": "string", "enum": ["file.read", "file.write", "file.delete",
            "file.stat", "file.list", "file.search"]},
        "resource": {"type": "string", "description": "Resolved absolute target path."},
        "attributes": {
            "type": "object", "required": ["scope", "path"],
            "properties": {
                "scope": {"type": "string", "enum": ["workspace", "external"]},
                "path": {"type": "string", "description": "Workspace-relative inside, absolute outside; POSIX separators."},
                "expected_version": {"type": "string"},
            },
        },
    },
}


__all__ = ["FILE_OPERATION_SCHEMA"]
