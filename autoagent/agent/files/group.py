"""Explicitly initialized, individually selectable file Tools."""

from ..tools.group import ToolGroup
from .service import FileService, LocalFileService
from .tools import file_tools


class FileTools(ToolGroup):
    group_id = 'files'

    def __init__(self, workspace=None, *, service=None, config=None, working_directory='.'):
        if service is not None and workspace is not None:
            raise ValueError('Pass a workspace directory or a FileService, not both.')
        if service is None:
            if workspace is None:
                raise ValueError('FileTools requires a directory or FileService.')
            service = LocalFileService(workspace, working_directory=working_directory, config=config)
        if not isinstance(service, FileService):
            raise TypeError('service must implement FileService.')
        self.service = service
        self._tools = file_tools(service)
        self._bind_group()
