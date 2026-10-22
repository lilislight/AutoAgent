"""HTTP execution transport; mutations are never automatically retried."""

import asyncio
from pathlib import Path
from urllib.parse import urlsplit

from . import CliError
from .execution import action_options, document


def _check_operation(receipt):
    if receipt["status"] == "unobserved":
        raise CliError("OUTCOME_UNKNOWN", "The operation observer closed. Check execution state before retrying.", exit_code=4)
    if receipt["status"] == "failed":
        error = receipt.get("error") or {}
        raise CliError(error.get("code", "OPERATION_FAILED"), error.get("message", "Execution operation failed."), exit_code=3)


class ServerClient:
    def __init__(self, url, *, timeout=30, transport=None):
        try:
            import httpx
        except ModuleNotFoundError as error:
            raise CliError("DEPENDENCY_MISSING", "Install autoagent[cli] for HTTP execution.", exit_code=3) from error
        try:
            parts = urlsplit(url)
        except ValueError as error:
            raise CliError("USAGE_ERROR", "--server is not a valid HTTP(S) API base URL.") from error
        if parts.scheme not in ("http", "https") or not parts.hostname or parts.username or parts.password or parts.query or parts.fragment:
            raise CliError("USAGE_ERROR", "--server must be an HTTP(S) API base URL without credentials, query or fragment.")
        try:
            parts.port
        except ValueError as error:
            raise CliError("USAGE_ERROR", str(error)) from error
        self.url = url.rstrip("/")
        self.timeout = timeout
        self.httpx = httpx
        self.client = httpx.AsyncClient(timeout=timeout, transport=transport, follow_redirects=False)

    async def __aenter__(self):
        await self.client.__aenter__()
        return self

    async def __aexit__(self, *args):
        return await self.client.__aexit__(*args)

    async def request(self, method, route, *, params=None, body=None):
        mutation = method != "GET"
        try:
            response = await self.client.request(method, self.url + route, params=params, json=body)
        except self.httpx.TransportError as error:
            code = "OUTCOME_UNKNOWN" if mutation else "SERVER_UNAVAILABLE"
            raise CliError(code, "Submission outcome is unknown; do not resubmit automatically." if mutation else "Cannot read execution state from Server.",
                exit_code=4 if mutation else 3) from error
        if response.status_code >= 500 and mutation:
            raise CliError("OUTCOME_UNKNOWN", "Server could not confirm submission. Check execution state before retrying.", exit_code=4,
                details={"http_status": response.status_code})
        if response.status_code == 204 and method == 'DELETE':
            return {}
        try:
            data = response.json()
        except ValueError as error:
            raise CliError("OUTCOME_UNKNOWN" if mutation else "INVALID_SERVER_RESPONSE", "Server returned a non-JSON response.", exit_code=4 if mutation else 3) from error
        if not 200 <= response.status_code < 300:
            detail = data.get("detail", data) if isinstance(data, dict) else data
            message = detail.get("message", str(detail)) if isinstance(detail, dict) else str(detail)
            code = detail.get("code", "HTTP_ERROR") if isinstance(detail, dict) else "HTTP_ERROR"
            raise CliError(code, message, exit_code=2 if response.status_code in (400, 404, 409, 422) else 3,
                details={"http_status": response.status_code, "detail": detail})
        if not isinstance(data, dict):
            raise CliError("OUTCOME_UNKNOWN" if mutation else "INVALID_SERVER_RESPONSE", "Server returned an invalid response document.", exit_code=4 if mutation else 3)
        return data

    async def observe(self, iid, *, until, deadline):
        loop = asyncio.get_running_loop()
        while True:
            remaining = None if deadline is None else max(0, deadline - loop.time())
            state = await self.request("GET", "/execution-state", params={
                "invocation_id": iid, "until": until, "timeout": min(1, self.timeout / 2, remaining) if remaining is not None else min(1, self.timeout / 2)})
            if state["observation"] != "timeout" or remaining is not None and loop.time() >= deadline:
                return state
            # The Server may return early under a short query timeout; avoid a hot retry loop.
            await asyncio.sleep(min(0.1, max(0, deadline - loop.time())) if deadline is not None else 0.1)


async def run_remote(options, value):
    source = dict(kind="server", url=options.server_url.rstrip("/"))
    loop = asyncio.get_running_loop()
    async with ServerClient(options.server_url, timeout=options.request_timeout) as client:
        options._source = source
        if options.group == "invoke":
            revision = options.revision_id
            if revision is None:
                page = await client.request("GET", "/workflows", params={"workflow_id": options.workflow_id, "limit": 1})
                if not page["items"]:
                    raise CliError("WORKFLOW_NOT_FOUND", f"Workflow is not registered: {options.workflow_id}")
                revision = page["items"][0]["revision_id"]
            options._execution = dict(workflow_id=options.workflow_id, revision_id=revision, entry_node_id=options.entry_node)
            if options.session_id:
                options._execution["session_id"] = options.session_id
            receipt = await client.request("POST", "/executions", body=dict(workflow_id=options.workflow_id,
                revision_id=revision, entry_node_id=options.entry_node, input=value,
                session_mode=options.session_mode, session_id=options.session_id))
            if receipt.get("accepted") is not True or not all(isinstance(receipt.get(key), str) and receipt[key]
                    for key in ("invocation_id", "session_id", "workflow_id", "revision_id")):
                raise CliError("OUTCOME_UNKNOWN", "Server did not return a valid execution receipt. Do not resubmit automatically.", exit_code=4)
            options._execution = {key: receipt[key] for key in ("invocation_id", "session_id", "revision_id") if key in receipt}
            iid = receipt["invocation_id"]
            deadline = None if options.wait_timeout is None else loop.time() + options.wait_timeout
            if options.wait == "none":
                state = dict(receipt, observation="accepted")
            else:
                state = await client.observe(iid, until=options.wait, deadline=deadline)
        else:
            controls = await client.request("GET", "/execution-controls", params={"invocation_id": options.invocation_id})
            iid = controls["root_invocation_id"]
            options._execution = dict(invocation_id=iid, session_id=controls["root_session_id"])
            kwargs = action_options(options, controls, value)
            if options.command == "recover" and options.request_id and options.expected_version is None:
                # An explicit retry must preserve the original precondition,
                # rather than fingerprinting the current post-recovery version.
                try:
                    previous = await client.request("GET", "/execution-operations/" + options.request_id)
                except CliError as error:
                    if error.code != "operation_unavailable":
                        raise
                else:
                    if previous.get("expected_version") is not None:
                        kwargs["expected_version"] = previous["expected_version"]
            if "expected_version" in kwargs:
                options._execution["submitted_expected_version"] = kwargs["expected_version"]
            receipt = await client.request("POST", "/execution-actions", body=dict(invocation_id=options.invocation_id, action=options.command, **kwargs))
            if receipt.get("operation_id") != options._request_id or receipt.get("status") not in ("running", "succeeded", "failed", "unobserved"):
                raise CliError("OUTCOME_UNKNOWN", "Server returned an invalid operation receipt. Do not resubmit automatically.", exit_code=4)
            _check_operation(receipt)
            deadline = None if options.wait_timeout is None else loop.time() + options.wait_timeout
            if options.wait == "none":
                state = dict(invocation_id=iid, operation_id=receipt["operation_id"], observation="accepted")
            else:
                while receipt["status"] == "running":
                    if deadline is not None and loop.time() >= deadline:
                        state = dict(await client.request("GET", "/execution-state", params={"invocation_id": iid}),
                            observation="timeout", operation_id=receipt["operation_id"])
                        break
                    await asyncio.sleep(min(0.1, max(0, deadline - loop.time())) if deadline is not None else 0.1)
                    try:
                        receipt = await client.request("GET", "/execution-operations/" + receipt["operation_id"])
                    except CliError as error:
                        if error.code == "operation_unavailable":
                            raise CliError("OUTCOME_UNKNOWN", "Operation receipt expired or Server restarted. Check execution state before retrying.",
                                exit_code=4, details={"operation_id": options._request_id}) from error
                        raise
                else:
                    _check_operation(receipt)
                    if receipt["status"] != "succeeded":
                        raise CliError("INVALID_SERVER_RESPONSE", "Server returned an invalid operation state.", exit_code=3)
                    state = await client.observe(iid, until=options.wait, deadline=deadline)
                    state["operation_id"] = receipt["operation_id"]
            state["request_id"] = options._request_id
            if "expected_version" in kwargs:
                state["submitted_expected_version"] = kwargs["expected_version"]
        from .execution import failure_details
        await failure_details(state,
            lambda: client.request("GET", "/execution-value", params=dict(invocation_id=iid,
                kind="invocation", identity=iid, field="error", pointer="/message")),
            lambda: client.request("GET", "/invocation-diagnosis", params=dict(invocation_id=iid, limit=3)))
        state.update(host_state="server", execution_continues=state.get("status") not in ("completed", "failed", "cancelled") and state.get("control_state") != "paused")
        return Path.cwd(), *document(options, source, state)
