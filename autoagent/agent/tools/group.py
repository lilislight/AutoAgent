"""Selection and lifecycle shared by ordinary SDK Tool groups."""


class ToolGroup:
    group_id = None

    def tools(self, *, include=None, exclude=()):
        selected = self._tools.select(include=include, exclude=exclude)
        return tuple(selected[name] for name in selected._tools)

    def __getitem__(self, name):
        return self._tools[name]

    def _bind_group(self):
        for tool in self._tools._tools.values():
            tool.group = self
