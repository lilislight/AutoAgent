"""Structural services and optional execution extensions, independent of products."""

def walk_workflows(workflows):
    """Visit root and embedded Workflow definitions once, without compiling them."""
    pending, seen = list(workflows), set()
    while pending:
        workflow = pending.pop()
        if id(workflow) in seen:
            continue
        seen.add(id(workflow))
        yield workflow
        pending.extend(child for node in workflow.nodes
            if (child := getattr(node.executable, 'workflow', None)) is not None
            and not isinstance(child, str))


class RuntimeServices:
    """Borrow Host facilities without handing extensions its implementation internals."""

    def __init__(self, host):
        self._host = host

    @property
    def runtime(self):
        return self._host._runtime

    @property
    def persistence(self):
        return self._host._persistence


    @property
    def providers(self):
        return self._host.providers

    def extension(self, name):
        return self._host.extensions[name]

    async def perform(self, operation, **options):
        return await self._host._perform(operation, **options)

    def execution(self, execution, **options):
        return self._host._execution(execution, **options)

    def conflict(self, code, message):
        from .models import HostRunConflict
        return HostRunConflict(code, message)

    async def run(self, operation):
        """Execute on the loop owning execution resources."""
        import asyncio
        loop = self.runtime.resource_loop
        if loop is None or loop is asyncio.get_running_loop():
            return await operation()
        return await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(operation(), loop))


class RuntimeExtensions:
    def __init__(self, services):
        self.services = services
        self.items = {}
        self._entered = set()

    def discover(self, workflows):
        for workflow in walk_workflows(workflows):
            for node in workflow.nodes:
                factory = getattr(getattr(node.executable, 'handler', None), '__runtime_extension__', None)
                if factory is not None and factory.id not in self.items:
                    self.items[factory.id] = factory(self.services)

    async def enter_lifespans(self, setup, stack):
        for key, extension in tuple(self.items.items()):
            if key not in self._entered:
                await stack.enter_async_context(extension.lifespan(setup))
                self._entered.add(key)
                stack.callback(self._entered.discard, key)
    async def enter_resources(self, setup, stack):
        for extension in self.items.values():
            scope = getattr(extension, "resource_scope", None)
            if scope is not None:
                await stack.enter_async_context(scope(setup))

    async def call(self, method, *args):
        for extension in self.items.values():
            handler = getattr(extension, method, None)
            if handler is not None:
                await handler(*args)

    async def project(self, result):
        for extension in self.items.values():
            result = await extension.project(result)
        return result

    async def respond(self, ref, wait_id, response):
        for extension in self.items.values():
            if await extension.respond(ref, wait_id, response):
                return
        await self.services.runtime.app.asubmit_resume(ref, wait_id, response)

    async def action_view(self, ref, wait_id):
        for extension in self.items.values():
            view = await extension.action_view(ref, wait_id)
            if view is not None:
                return view
        status = await self.services.runtime.app.astatus(ref)
        wait = next((item for item in status.waits if item.id == wait_id), None)
        return dict(request=wait.request if wait else None, response_example=None)

    def allows_recovery(self, revision, status, **options):
        return all(extension.allows_recovery(revision, status, **options) for extension in self.items.values())
