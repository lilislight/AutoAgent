"""File-operation facts advertised to permission editors, never to the model.

resource is the resolved absolute target. attributes.path is Workspace-relative
inside the root and absolute outside it, using POSIX separators. Directory
identity and path revalidation data stay in the private execution binding.
"""


FILE_OPERATION_SCHEMA = {
    "type": "object",
    "required": ["action", "resource", "attributes"],
    "properties": {
        "action": {"type": "string", "enum": ["file.inspect", "file.read", "file.write", "file.delete"]},
        "resource": {"type": "string", "description": "Resolved absolute target path."},
        "attributes": {
            "type": "object", "required": ["scope", "path"],
            "properties": {
                "scope": {"type": "string", "enum": ["workspace", "external"]},
                "path": {"type": "string", "description": "Workspace-relative inside, absolute outside; POSIX separators."},
            },
            "additionalProperties": False,
        },
    },
}


__all__ = ["FILE_OPERATION_SCHEMA"]
