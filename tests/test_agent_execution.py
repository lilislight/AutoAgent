"""Real process lifecycle, conservative command approval and Host integration."""

import asyncio
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

from pydantic import ValidationError

from autoagent.agent import (
    AgentApprovalRequest, AgentApprovalResponse, AgentInput, CommandRule, ExecutionToolsConfig,
    LLMResponse, LLMToolCall, LocalExecutionService, ProcessManager, ToolContext,
    ToolApprovalRequired, ToolDecision, TreeSitterShellAnalyzer, WorkspaceBinding, agent_workflow, execution_tools,
)
from autoagent.agent.context import restore
from autoagent.host import AutoAgentHost, HostSettings
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_workflow import ScriptedProvider, answer, settings


def call(name="execute_command", **arguments):
    return LLMToolCall(name=name, call_id=name, arguments=json.dumps(arguments))


def python(code):
    return {"mode": "exec", "program": sys.executable, "args": ["-u", "-c", code]}


class OutputLogTests(unittest.TestCase):
    def test_utf8_log_cap_retains_a_contiguous_prefix(self):
        """A byte cap drops incomplete UTF-8 and all subsequent chunks, never splicing output."""
        import io
        from autoagent.agent.execution.logs import OutputLog
        stream = io.BytesIO()
        log = OutputLog("log", stream, 4)
        log.write("中中".encode())
        log.write(b"later")
        self.assertEqual(stream.getvalue(), "中".encode())
        self.assertTrue(log.summary()["log_truncated"])
        log.close()
        self.assertTrue(stream.closed)

    def test_log_write_failure_is_reported_without_interrupting_output_drain(self):
        """Disk failure is retained in the result and subsequent chunks remain harmless."""
        from autoagent.agent.execution.logs import OutputLog
        class FailedStream:
            def write(self, value):
                raise OSError("disk full")
            def close(self):
                pass
        log = OutputLog("log", FailedStream(), 100)
        log.write(b"one")
        log.write(b"two")
        log.close()
        self.assertEqual(log.summary()["log_error"], "disk full")


class ShellAnalysisTests(unittest.TestCase):
    def test_literal_chains_parse_in_both_languages(self):
        """Both grammar adapters identify static commands and preserve quoting."""
        analyzer = TreeSitterShellAnalyzer()
        for shell in ("bash", "pwsh"):
            with self.subTest(shell=shell):
                result = analyzer.analyze("git status --short && git diff 'some file'", shell)
                self.assertTrue(result.complete, result.reason)
                self.assertEqual(result.commands, (("git", "status", "--short"), ("git", "diff", "some file")))

    def test_dynamic_and_side_effecting_syntax_is_opaque(self):
        """Partial command extraction never authorizes unsupported script effects."""
        analyzer = TreeSitterShellAnalyzer()
        scripts = ["git status > x", "git $(whoami)", "$CMD status", "git status &", "git status &&",
            "git status; cd /tmp; git diff", "X=1; git status", "git *", "for x in a; do git status; done"]
        for script in scripts:
            with self.subTest(script=script):
                self.assertFalse(analyzer.analyze(script, "bash").complete)
        for script in ["git $env:SECRET", "git status > x", "git --% %SECRET%", "& $cmd status", "$x=1; git status"]:
            with self.subTest(script=script):
                self.assertFalse(analyzer.analyze(script, "pwsh").complete)


class ExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.service = self.make_service(ExecutionToolsConfig(default_decision="allow"))
        self.tools = execution_tools(self.service, context=ToolContext(session_id="one"))

    def make_service(self, config=None, **kwargs):
        service = LocalExecutionService(self.root, config=config, **kwargs)
        self.addAsyncCleanup(service.aclose)
        return service

    async def invoke(self, command=None, **kwargs):
        return await self.tools.ainvoke(call(command=command or python("print('hello')"), **kwargs))

    async def manage(self, action, **kwargs):
        return await self.tools.ainvoke(call("manage_process", request={"action": action, **kwargs}))

    async def test_wait_awaits_exit_and_expiry_does_not_stop_process(self):
        """Exit waits ignore intermediate output and finite expiry preserves a running handle."""
        result = await self.invoke(python("import time; print('started'); time.sleep(.3); print('done')"), yield_seconds=0)
        pid = result.structured_content["process_id"]
        pending = await self.manage("wait", process_id=pid, wait_seconds=.01)
        self.assertEqual(pending.structured_content["state"], "running")
        final = await self.manage("wait", process_id=pid, wait_seconds=2, cursor=0)
        self.assertEqual(final.structured_content["exit_code"], 0)
        self.assertIn("done", final.structured_content["stdout"])
        invalid = await self.manage("wait", process_id=pid, wait_seconds=601)
        self.assertEqual(invalid.structured_content["error"]["kind"], "invalid_wait")

    async def test_logs_preserve_exit_status_and_bound_preview_without_overwrite(self):
        """Captured output survives preview truncation and never replaces an existing log."""
        result = await self.invoke(python("print('x'*12000); raise SystemExit(7)"), log_path="logs/run.txt", yield_seconds=2)
        value = result.structured_content
        self.assertTrue(result.is_error)
        self.assertEqual(value["exit_code"], 7)
        self.assertLessEqual(len(value["stdout"].encode()), 4096)
        log = self.root / "logs/run.txt"
        self.assertEqual(log.read_text(), "x"*12000 + "\n")
        duplicate = await self.invoke(log_path="logs/run.txt")
        self.assertEqual(duplicate.structured_content["error"]["kind"], "log_exists")
        self.assertEqual(log.stat().st_size, 12001)

    async def test_managed_directories_redirect_cache_and_temp_inside_workspace(self):
        """Optional cache/temp controls create authorized directories and set library conventions."""
        self.service = self.make_service(ExecutionToolsConfig(default_decision="allow",
            cache_directory=".cache", temp_directory=".tmp"))
        self.tools = execution_tools(self.service, context=ToolContext(session_id="one"))
        result = await self.invoke(python("import os,tempfile,json; print(json.dumps([os.environ['HF_HOME'],os.environ['PIP_CACHE_DIR'],tempfile.gettempdir()]))"))
        self.assertFalse(result.is_error, result)
        paths = json.loads(result.structured_content["stdout"])
        self.assertEqual(paths, [str(self.root / ".cache/huggingface"), str(self.root / ".cache/pip"), str(self.root / ".tmp")])
        invalid = await self.invoke(cache_directory=str(self.root.parent))
        self.assertEqual(invalid.structured_content["error"]["kind"], "invalid_output_path")
        collision = await self.invoke(log_path="future", cache_directory="future/cache")
        self.assertEqual(collision.structured_content["error"]["kind"], "invalid_output_path")
        self.assertFalse((self.root / "future").exists())
        sensitive = call(command=python("print('no')"), log_path=".env")
        self.assertEqual((await self.tools.acheck(sensitive)).action, "ask")
        with self.assertRaises(ToolApprovalRequired):
            await self.tools.ainvoke(sensitive)
        self.assertFalse((self.root / ".env").exists())

    def test_configured_limits_reach_model_schema_without_cross_instance_leakage(self):
        """Each Tool schema exposes its own runtime wait, timeout and preview bounds."""
        service = self.make_service(ExecutionToolsConfig(max_wait_seconds=80, default_wait_seconds=45, max_input_bytes=512))
        definitions = {value.name: value.parameters for value in execution_tools(service).definitions}
        self.assertEqual(definitions["execute_command"]["properties"]["yield_seconds"]["anyOf"][0]["maximum"], 80)
        wait = definitions["manage_process"]["$defs"]["ProcessWait"]["properties"]["wait_seconds"]
        self.assertEqual(wait["default"], 45)
        self.assertEqual(definitions["execute_command"]["properties"]["stdin"]["anyOf"][0]["maxLength"], 512)
        self.assertEqual(definitions["manage_process"]["$defs"]["ProcessWrite"]["properties"]["text"]["maxLength"], 512)
        original = {value.name: value.parameters for value in self.tools.definitions}
        self.assertEqual(original["execute_command"]["properties"]["yield_seconds"]["anyOf"][0]["maximum"], 600)
        self.assertIn("literal", original["execute_command"]["$defs"]["ExecCommand"]["properties"]["args"]["description"])
        self.assertIn("deadline", original["execute_command"]["properties"]["timeout_seconds"]["description"])
        self.assertIn("previous next_cursor", original["manage_process"]["$defs"]["ProcessRead"]["properties"]["cursor"]["description"])

    def test_typed_contract_and_required_decisions(self):
        """Rule decisions are mandatory and both command/request unions are typed."""
        with self.assertRaises(ValidationError):
            CommandRule(program="git")
        with self.assertRaises(ValidationError):
            CommandRule(program="git", decision="allow", args=[], args_prefix=[])
        with self.assertRaises(ValidationError):
            CommandRule(program="git", decision="allow", args_regex="(")
        config = ExecutionToolsConfig(rules=[CommandRule(program="git", decision="ask")])
        self.assertEqual(ExecutionToolsConfig.model_validate_json(config.model_dump_json()), config)
        definitions = {value.name: value.parameters for value in self.tools.definitions}
        self.assertEqual(set(definitions), {"execute_command", "manage_process"})
        self.assertIn("oneOf", str(definitions["execute_command"]))

    def test_workspace_content_changes_do_not_change_tool_version(self):
        """Ordinary file creation does not invalidate an unchanged execution capability."""
        before = self.make_service().version
        (self.root / "unrelated").write_text("new file")
        self.assertEqual(self.make_service().version, before)

    async def test_exec_does_not_interpret_shell_characters(self):
        """Pipes and substitutions remain literal arguments in direct execution."""
        result = await self.invoke({"mode": "exec", "program": sys.executable,
            "args": ["-c", "import sys; print(sys.argv[1])", "hello | $(whoami)"]})
        self.assertFalse(result.is_error, result)
        self.assertEqual(result.structured_content["stdout"], "hello | $(whoami)\n")
        self.assertTrue(result.structured_content["process_id"])
        self.assertIn("next_cursor", result.structured_content)
        self.assertFalse(result.structured_content["has_more"])
        for field in ("stderr", "error", "reason", "truncated", "wall_time_seconds", "total_output_bytes", "retained_start"):
            self.assertNotIn(field, result.structured_content)
        self.assertGreater(result.tool_metadata["execution"]["total_output_bytes"], 0)

    async def test_admission_rejection_does_not_create_log_or_cache_directories(self):
        """A rejected launch has no preparation side effects and its log path remains usable later."""
        service = self.make_service(ExecutionToolsConfig(default_decision="allow", max_running_processes=1))
        tools = execution_tools(service, context=ToolContext(session_id="one"))
        first = await tools.ainvoke(call(command=python("import time; time.sleep(60)"), yield_seconds=0))
        process_id = first.structured_content["process_id"]
        request = call(command=python("print('ready')"), log_path="logs/retry.txt", cache_directory="cache")
        rejected = await tools.ainvoke(request)
        self.assertEqual(rejected.structured_content["error"]["kind"], "process_limit_exceeded")
        self.assertIn("not started", rejected.structured_content["error"]["message"])
        self.assertIn(process_id, rejected.structured_content["error"]["message"])
        self.assertFalse((self.root / "logs").exists())
        self.assertFalse((self.root / "cache").exists())
        await service.manager.get(process_id, "one", str(self.root)).stop()
        result = await tools.ainvoke(request)
        self.assertFalse(result.is_error, result)
        self.assertEqual((self.root / "logs/retry.txt").read_text(), "ready\n")

    async def test_incremental_read_wakes_on_output_and_list_describes_owned_process(self):
        """A read waits for new bytes without polling and list identifies the command directory and outcome."""
        started = await self.invoke(python("import time; time.sleep(.25); print('new',flush=True); time.sleep(60)"), yield_seconds=0)
        pid = started.structured_content["process_id"]
        result = await self.manage("read", process_id=pid, cursor=started.structured_content["next_cursor"], wait_seconds=2)
        self.assertIn("new", result.structured_content["stdout"])
        self.assertEqual(result.structured_content["state"], "running")
        self.assertFalse(result.structured_content["has_more"])
        self.assertEqual(result.structured_content["cwd"], str(self.root))
        listed = (await self.manage("list")).structured_content["processes"][0]
        self.assertEqual(listed["process_id"], pid)
        self.assertEqual(listed["cwd"], str(self.root))
        self.assertGreaterEqual(listed["elapsed_seconds"], 0)
        await self.manage("terminate", process_id=pid)
        listed = (await self.manage("list")).structured_content["processes"][0]
        self.assertIn("exit_code", listed)

    async def test_failed_preparation_releases_reserved_process_capacity(self):
        """A log-open failure leaves no reserved handle and cannot block the next launch."""
        service = self.make_service(ExecutionToolsConfig(default_decision="allow", max_running_processes=1))
        tools = execution_tools(service, context=ToolContext(session_id="one"))
        with patch.object(service.files, "open_output", side_effect=OSError("disk unavailable")):
            result = await tools.ainvoke(call(command=python("print('ready')"), log_path="run.txt"))
        self.assertEqual(result.structured_content["error"]["kind"], "process_start_failed")
        self.assertFalse(service.manager.records())
        result = await tools.ainvoke(call(command=python("print('ready')")))
        self.assertFalse(result.is_error, result)

    async def test_trimmed_output_reports_skipped_bytes_without_replaying_it(self):
        """Preview truncation and expired cursors expose the missing byte count alongside continuation."""
        service = self.make_service(ExecutionToolsConfig(default_decision="allow", max_retained_bytes=40, max_output_bytes=20))
        tools = execution_tools(service, context=ToolContext(session_id="one"))
        result = await tools.ainvoke(call(command=python("print('x'*100)")))
        value = result.structured_content
        self.assertEqual(value["skipped_bytes"], 81)
        read = await tools.ainvoke(call("manage_process", request=dict(action="read", process_id=value["process_id"], cursor=0)))
        self.assertEqual(read.structured_content["skipped_bytes"], 61)
        self.assertTrue(read.structured_content["has_more"])

    @unittest.skipUnless(shutil.which("bash"), "Bash is not installed")
    async def test_one_rule_authorizes_exec_shell_and_wrapper(self):
        """A single program/argument rule is shared by static Shell entry points."""
        config = ExecutionToolsConfig(default_decision="deny", rules=[
            CommandRule(program="printf", args_prefix=["%s"], decision="allow")])
        tools = execution_tools(self.make_service(config), context=ToolContext(session_id="one"))
        commands = [{"mode": "exec", "program": "printf", "args": ["%s", "hello"]},
            {"mode": "shell", "shell": "bash", "script": "printf '%s' hello"},
            {"mode": "exec", "program": "bash", "args": ["-c", "printf '%s' hello"]}]
        for command in commands:
            with self.subTest(command=command):
                self.assertEqual((await tools.acheck(call(command=command))).action, "allow")
                result = await tools.ainvoke(call(command=command))
                self.assertEqual(result.structured_content["stdout"], "hello", result)

    @unittest.skipUnless(shutil.which("bash"), "Bash is not installed")
    async def test_compound_deny_prevents_all_execution(self):
        """A denied later command blocks even the allowed first command's effects."""
        tools = execution_tools(self.make_service(ExecutionToolsConfig(rules=[
            CommandRule(program=sys.executable, decision="allow"), CommandRule(program="false", decision="deny")])),
            context=ToolContext(session_id="one"))
        import shlex
        first = shlex.join([sys.executable, "-c", "from pathlib import Path; Path('marker').touch()"])
        result = await tools.ainvoke(call(command={"mode": "shell", "shell": "bash", "script": first + " && false"}))
        self.assertTrue(result.is_error)
        self.assertFalse((self.root / "marker").exists())

    async def test_rules_support_program_prefix_regex_and_default(self):
        """Ordered argument rules override program defaults; unmatched commands ask."""
        tools = execution_tools(self.make_service(ExecutionToolsConfig(rules=[
            CommandRule(program=sys.executable, decision="deny"),
            CommandRule(program=sys.executable, args_prefix=["--version"], decision="allow"),
            CommandRule(program=sys.executable, args_regex=r"-V", decision="ask")])) ,context=ToolContext(session_id="one"))
        for args, expected in [([], "deny"), (["--version"], "allow"), (["-V"], "ask")]:
            request = call(command={"mode": "exec", "program": sys.executable, "args": args})
            self.assertEqual((await tools.acheck(request)).action, expected)

    async def test_unconfigured_defaults_ask_and_deny_are_distinct(self):
        """The configured default applies only when no command rule matched."""
        for decision in ("ask", "deny"):
            tools = execution_tools(self.make_service(ExecutionToolsConfig(default_decision=decision)),
                context=ToolContext(session_id="one"))
            self.assertEqual((await tools.acheck(call(command=python("pass")))).action, decision)

    @unittest.skipUnless(shutil.which("bash"), "Bash is not installed")
    async def test_opaque_shell_cannot_inherit_an_inner_allow(self):
        """Redirection and expansion fall back to whole-script policy."""
        tools = execution_tools(self.make_service(ExecutionToolsConfig(rules=[
            CommandRule(program="printf", decision="allow")])), context=ToolContext(session_id="one"))
        for script in ["printf hello > marker", "printf $HOME", "printf hello && eval 'false'"]:
            checked = await tools.acheck(call(command={"mode": "shell", "shell": "bash", "script": script}))
            self.assertEqual(checked.action, "ask")
        self.assertFalse((self.root / "marker").exists())

    async def test_approval_binds_arguments_and_executable_identity(self):
        """An approval cannot authorize changed arguments or an executable swap."""
        tools = execution_tools(self.make_service(), context=ToolContext(session_id="one"))
        request = call(command=python("print('first')"))
        decision = await tools.acheck(request)
        reply = decision.approval_request.respond(approved=True)
        mismatch = await tools.ainvoke(call(command=python("print('second')")), approval=reply)
        self.assertTrue(mismatch.is_error)
        result = await tools.ainvoke(request, approval=reply)
        self.assertEqual(result.structured_content["stdout"], "first\n")

    async def test_resolved_path_does_not_trust_another_same_named_executable(self):
        """A bare program allow rule is tied to its PATH-resolved executable."""
        if os.name != "posix":
            self.skipTest("POSIX executable fixture")
        fake = self.root / Path(sys.executable).name
        fake.write_text("#!/bin/sh\nprintf fake")
        fake.chmod(0o700)
        tools = execution_tools(self.make_service(ExecutionToolsConfig(default_decision="deny",
            rules=[CommandRule(program=Path(sys.executable).name, decision="allow")])), context=ToolContext(session_id="one"))
        checked = await tools.acheck(call(command={"mode": "exec", "program": str(fake)}))
        self.assertEqual(checked.action, "deny")

    async def test_unknown_program_and_invalid_timeout_are_observations(self):
        """Preflight errors return typed observations without spawning anything."""
        for arguments, code in [({"command": {"mode": "exec", "program": "autoagent-program-does-not-exist"}}, "program_unavailable"),
            ({"command": python("pass"), "timeout_seconds": 99999}, "invalid_timeout")]:
            result = await self.tools.ainvoke(call(**arguments))
            self.assertEqual(result.structured_content["error"]["kind"], code)

    async def test_nonzero_exit_is_observed_without_retry(self):
        """An unsuccessful command executes once and exposes its real exit code."""
        result = await self.invoke(python("from pathlib import Path; p=Path('counter'); p.write_text(p.read_text()+'x' if p.exists() else 'x'); raise SystemExit(3)"))
        self.assertTrue(result.is_error)
        self.assertEqual(result.structured_content["exit_code"], 3)
        self.assertEqual((self.root / "counter").read_text(), "x")

    async def test_yield_returns_same_process_and_nonconsuming_cursors(self):
        """Returning early preserves the process; independent readers see the same bytes."""
        result = await self.invoke(python("import time; print('first',flush=True); time.sleep(.3); print('last')"), yield_seconds=0)
        process_id = result.structured_content["process_id"]
        self.assertEqual(result.structured_content["state"], "running")
        record = self.service.manager.get(process_id, "one", str(self.root))
        await record.handle.wait()
        first = await self.manage("read", process_id=process_id, cursor=0)
        second = await self.manage("read", process_id=process_id, cursor=0)
        self.assertEqual(first.structured_content["stdout"], "first\nlast\n")
        self.assertEqual(first.structured_content, second.structured_content)
        tail = await self.manage("read", process_id=process_id, cursor=first.structured_content["next_cursor"])
        self.assertEqual(tail.structured_content.get("stdout", ""), "")

    async def test_timeout_stops_process_and_retains_earlier_output(self):
        """Hard execution timeout differs from yielding and records its termination reason."""
        result = await self.invoke(python("import time; print('started',flush=True); time.sleep(60)"), timeout_seconds=.15)
        self.assertEqual(result.structured_content["state"], "timed_out")
        self.assertIn("started", result.structured_content["stdout"])

    async def test_bounded_output_marks_lost_history(self):
        """Large output cannot grow retained memory and expired cursors are explicit."""
        service = self.make_service(ExecutionToolsConfig(default_decision="allow", max_retained_bytes=100, max_output_bytes=40))
        tools = execution_tools(service, context=ToolContext(session_id="one"))
        result = await tools.ainvoke(call(command=python("print('x'*10000)")))
        self.assertTrue(result.structured_content["truncated"])
        process_id = result.structured_content["process_id"]
        result = await tools.ainvoke(call("manage_process", request={"action": "read", "process_id": process_id, "cursor": 0}))
        value = result.structured_content
        self.assertTrue(value["truncated"])
        self.assertEqual(value["retained_start"], 9901)
        self.assertLessEqual(len(value["stdout"].encode()), 40)
        self.assertTrue(value["has_more"])

    async def test_utf8_pagination_never_splits_a_character(self):
        """Small pages and OS byte splits preserve Unicode without replacement artifacts."""
        service = self.make_service(ExecutionToolsConfig(default_decision="allow", max_output_bytes=4))
        tools = execution_tools(service, context=ToolContext(session_id="one"))
        result = await tools.ainvoke(call(command=python("import os; data='你好世界'.encode(); [os.write(1,bytes([b])) for b in data]")))
        process_id = result.structured_content["process_id"]
        cursor, pieces = 0, []
        while True:
            result = await tools.ainvoke(call("manage_process", request={"action": "read", "process_id": process_id, "cursor": cursor}))
            value = result.structured_content
            pieces.append(value["stdout"])
            self.assertGreater(value["next_cursor"], cursor)
            cursor = value["next_cursor"]
            if not value.get("has_more", False):
                break
        self.assertEqual("".join(pieces), "你好世界")

    async def test_executable_replacement_invalidates_approval(self):
        """Approving a local executable does not approve a subsequently replaced file."""
        if os.name != "posix":
            self.skipTest("POSIX executable fixture")
        path = self.root / "tool"
        shutil.copyfile(shutil.which("printf"), path)
        path.chmod(0o700)
        tools = execution_tools(self.make_service(), context=ToolContext(session_id="one"))
        request = call(command={"mode": "exec", "program": str(path), "args": ["%s", "approved"]})
        decision = await tools.acheck(request)
        path.write_bytes(path.read_bytes() + b" ")
        result = await tools.ainvoke(request, approval=decision.approval_request.respond(approved=True))
        self.assertEqual(result.structured_content["error"]["kind"], "tool_approval_mismatch")

    async def test_regex_evaluation_is_bounded(self):
        """Expensive user patterns cannot stall command preflight indefinitely."""
        service = self.make_service(ExecutionToolsConfig(regex_timeout_seconds=.001, rules=[
            CommandRule(program=sys.executable, args_regex=r"(?:a|aa)+", decision="allow")]))
        tools = execution_tools(service, context=ToolContext(session_id="one"))
        request = call(command={"mode": "exec", "program": sys.executable, "args": ["a" * 20000 + "b"]})
        decision = await tools.acheck(request)
        self.assertEqual(decision.action, "deny")
        self.assertIn("regex", decision.reason)

    async def test_initial_input_and_default_eof(self):
        """Initial stdin is delivered and then closed unless explicitly kept open."""
        result = await self.invoke(python("import sys; print(sys.stdin.read())"), stdin="hello")
        self.assertEqual(result.structured_content["stdout"], "hello\n")
        result = await self.invoke(python("import sys; print(repr(sys.stdin.read()))"))
        self.assertEqual(result.structured_content["stdout"], "''\n")

    async def test_interactive_pipe_input_requires_approval(self):
        """Pipe input can be written after startup using the same approval boundary."""
        started = await self.invoke(python("print(input())"), keep_stdin_open=True, yield_seconds=0)
        process_id = started.structured_content["process_id"]
        request = call("manage_process", request={"action": "write", "process_id": process_id,
            "text": "hello\n", "close_stdin": True})
        decision = await self.tools.acheck(request)
        self.assertEqual(decision.action, "ask")
        await self.tools.ainvoke(request, approval=decision.approval_request.respond(approved=True))
        await self.service.manager.get(process_id, "one", str(self.root)).handle.wait()
        result = await self.manage("read", process_id=process_id, cursor=0)
        self.assertEqual(result.structured_content["stdout"], "hello\n")

    async def test_other_sessions_cannot_read_write_or_stop_handles(self):
        """Opaque process IDs never confer authority across Sessions."""
        started = await self.invoke(python("import time; time.sleep(60)"), yield_seconds=0)
        process_id = started.structured_content["process_id"]
        other = execution_tools(self.service, context=ToolContext(session_id="two"))
        for action in ("read", "write", "terminate"):
            result = await other.ainvoke(call("manage_process", request={"action": action, "process_id": process_id}))
            self.assertEqual(result.structured_content["error"]["kind"], "process_unavailable")

    async def test_runtime_environment_does_not_inherit_application_secrets(self):
        """Only selected process environment fields are inherited by default."""
        with patch.dict(os.environ, {"AUTOAGENT_TEST_SECRET": "hidden", "BASH_ENV": "/evil/startup"}):
            service = self.make_service(ExecutionToolsConfig(default_decision="allow"))
        tools = execution_tools(service, context=ToolContext(session_id="one"))
        result = await tools.ainvoke(call(command=python("import os; print(os.getenv('AUTOAGENT_TEST_SECRET')); print(os.getenv('BASH_ENV'))")))
        self.assertEqual(result.structured_content["stdout"], "None\nNone\n")

    async def test_custom_async_policy_can_override_default(self):
        """Applications can inspect resolved operations through an async policy."""
        async def policy(operation, context):
            return ToolDecision(action="allow")
        tools = execution_tools(self.make_service(policy=policy), context=ToolContext(session_id="one"))
        result = await tools.ainvoke(call(command=python("print('custom')")))
        self.assertEqual(result.structured_content["stdout"], "custom\n")

    async def test_cancelled_launch_cleans_up_the_owned_process(self):
        """Cancellation during foreground waiting stops the command before propagating."""
        task = asyncio.create_task(self.invoke(python("import time; time.sleep(60)")))
        async with asyncio.timeout(3):
            while not self.service.manager.records():
                await asyncio.sleep(.01)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(any(self.service.manager.running(r) for r in self.service.manager.records()))

    async def test_cancel_during_backend_startup_still_owns_the_child(self):
        """A cancellation before the backend returns cannot orphan its new process."""
        from autoagent.agent import LocalExecutionBackend
        ready, release = asyncio.Event(), asyncio.Event()
        class Backend(LocalExecutionBackend):
            async def spawn(self, spec):
                self.handle = await super().spawn(spec)
                ready.set()
                await release.wait()
                return self.handle
        backend = Backend()
        service = self.make_service(ExecutionToolsConfig(default_decision="allow"), backend=backend)
        tools = execution_tools(service, context=ToolContext(session_id="one"))
        task = asyncio.create_task(tools.ainvoke(call(command=python("import time; time.sleep(60)"))))
        await asyncio.wait_for(ready.wait(), 3)
        task.cancel()
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(backend.handle.state, "terminated")

    async def test_cancelling_output_read_keeps_the_process_running(self):
        """Detaching a log reader does not terminate the independently managed command."""
        started = await self.invoke(python("import time; time.sleep(60)"), yield_seconds=0)
        process_id = started.structured_content["process_id"]
        task = asyncio.create_task(self.manage("read", process_id=process_id, cursor=0, wait_seconds=30))
        await asyncio.sleep(.03)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.service.manager.get(process_id, "one", str(self.root)).handle.state, "running")

    async def test_parallel_launches_respect_admission_limit(self):
        """Concurrent Map-like calls cannot oversubscribe the Session process cap."""
        service = self.make_service(ExecutionToolsConfig(default_decision="allow", max_running_processes=1))
        tools = execution_tools(service, context=ToolContext(session_id="one"))
        results = await asyncio.gather(*(tools.ainvoke(call(command=python("import time; time.sleep(60)"), yield_seconds=0))
            for _ in range(8)))
        self.assertEqual(sum(not result.is_error for result in results), 1)
        self.assertEqual(sum(service.manager.running(r) for r in service.manager.records()), 1)

    async def test_process_limits_and_manager_close_are_effective(self):
        """Per-Session limits reject extra starts; close terminates and invalidates handles."""
        service = self.make_service(ExecutionToolsConfig(default_decision="allow", max_running_processes=1))
        tools = execution_tools(service, context=ToolContext(session_id="one"))
        command = python("import time; time.sleep(60)")
        first = await tools.ainvoke(call(command=command, yield_seconds=0))
        result = await tools.ainvoke(call(command=command, yield_seconds=0))
        self.assertEqual(result.structured_content["error"]["kind"], "process_limit_exceeded")
        record = service.manager.get(first.structured_content["process_id"], "one", str(self.root))
        await service.aclose()
        self.assertEqual(record.handle.state, "terminated")
        self.assertFalse(service.manager.records())

    @unittest.skipUnless(os.name == "posix", "POSIX process-group cleanup")
    async def test_root_exit_reclaims_ordinary_background_children(self):
        """A child retaining pipes cannot leave the result waiting until the hard deadline."""
        result = await self.invoke(python("import subprocess,sys; subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)']); print('done')"), timeout_seconds=5)
        self.assertEqual(result.structured_content["state"], "exited")
        self.assertLess(result.tool_metadata["execution"]["wall_time_seconds"], 3)

    @unittest.skipUnless(os.name == "posix", "POSIX escaped descendant fixture")
    async def test_escaped_descendant_cannot_retain_host_pipe_descriptors(self):
        """Even best-effort cleanup closes owned pipes when a descendant escaped its group."""
        code = ("import subprocess,sys; from pathlib import Path; "
            "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'],start_new_session=True); "
            "Path('escaped-pid').write_text(str(p.pid))")
        try:
            result = await self.invoke(python(code), timeout_seconds=.2)
            record = self.service.manager.get(result.structured_content["process_id"], "one", str(self.root))
            self.assertTrue(record.handle.process._transport.is_closing())
        finally:
            path = self.root / "escaped-pid"
            if path.exists():
                import signal
                try:
                    os.kill(int(path.read_text()), signal.SIGKILL)
                except ProcessLookupError:
                    pass


class HostExecutionContracts:
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.url = await self.database_url()

    @asynccontextmanager
    async def host(self, provider, *, workflow=None):
        @asynccontextmanager
        async def lifespan(setup):
            setup.agent_context.resources.register_provider("offline", provider)
            setup.agent_context.set_defaults(settings())
            yield
        async with AutoAgentHost(workflows=[workflow or agent_workflow()], root=self.root,
            environment={}, lifespan=lifespan, settings=HostSettings(database_url=self.url)) as host:
            yield host

    async def bind(self, host, session="one"):
        await host.agent_context.configure(session, settings().session_defaults(), workspace=WorkspaceBinding(workspace_id=host.agent_context.project_workspace_id))

    async def test_workspace_and_build_control_tool_visibility(self):
        """Execution Tools appear only for bound, explicitly enabled builds."""
        provider = ScriptedProvider(answer(), answer())
        async with self.host(provider) as host:
            await self.bind(host)
            for session in ("one", "unbound"):
                result = await host.join(await host.execute("agent", AgentInput(message="hello"), session_id=session))
                self.assertEqual(result.status, "completed", result.error)
        self.assertIn("execute_command", [tool.name for tool in provider.requests[0].tools])
        self.assertNotIn("execute_command", [tool.name for tool in provider.requests[1].tools])
        provider = ScriptedProvider(answer())
        async with self.host(provider, workflow=agent_workflow(execution=False)) as host:
            await self.bind(host)
            await host.join(await host.execute("agent", AgentInput(message="hello"), session_id="one"))
        self.assertNotIn("execute_command", [tool.name for tool in provider.requests[0].tools])

    async def test_command_wait_approval_round_trip_and_history(self):
        """A real command starts only after batch approval and its result enters history."""
        command = python("from pathlib import Path; Path('approved').write_text('yes'); print('approved')")
        provider = ScriptedProvider(LLMResponse(model="offline", finish_reason="tool_calls", output=(call(command=command),)), answer())
        async with self.host(provider) as host:
            await self.bind(host)
            waiting = await host.join(await host.execute("agent", AgentInput(message="run"), session_id="one"))
            self.assertEqual(waiting.status, "waiting", waiting.error)
            self.assertFalse((self.root / "approved").exists())
            request = restore(AgentApprovalRequest, waiting.waits[0].request)
            self.assertEqual(request.requests[0].operations[0]["action"], "command.execute")
            reply = AgentApprovalResponse(decisions=tuple(v.respond(approved=True) for v in request.requests), revision=request.revision)
            await host.respond(waiting.invocation_id, waiting.waits[0].id, reply.model_dump(mode="json"))
            result = await host.join(waiting.invocation_id)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual((self.root / "approved").read_text(), "yes")
        self.assertIn("approved", str(provider.requests[-1].input))

    async def test_restart_requires_fresh_binding_and_approval_before_execution(self):
        """Restart invalidates old dynamic bindings; recovery can request a fresh call safely."""
        command = python("from pathlib import Path; p=Path('counter'); p.write_text(p.read_text()+'x' if p.exists() else 'x')")
        provider = ScriptedProvider(LLMResponse(model="offline", finish_reason="tool_calls", output=(call(command=command),)))
        async with self.host(provider) as host:
            await self.bind(host)
            waiting = await host.join(await host.execute("agent", AgentInput(message="run"), session_id="one"))
            self.assertEqual(waiting.status, "waiting", waiting.error)
        (self.root / "unrelated").touch()
        provider = ScriptedProvider(LLMResponse(model="offline", finish_reason="tool_calls", output=(call(command=command),)), answer())
        async with self.host(provider) as host:
            current = await host.agent_context.get_approval(waiting.invocation_id, waiting.waits[0].id)
            self.assertFalse(current.requests)
            self.assertEqual(current.automatic[0].result.structured_content["error"]["kind"], "tool_binding_changed")
            self.assertFalse((self.root / "counter").exists())
            recovered = await host.recover(waiting.invocation_id)
            self.assertEqual(recovered.status, "waiting", recovered.error)
            current = await host.agent_context.get_approval(waiting.invocation_id, recovered.waits[0].id)
            self.assertEqual(len(current.requests), 1)
            reply = AgentApprovalResponse(decisions=tuple(v.respond(approved=True) for v in current.requests), revision=current.revision)
            await host.respond(waiting.invocation_id, recovered.waits[0].id, reply.model_dump(mode="json"))
            result = await host.join(waiting.invocation_id)
            self.assertEqual(result.status, "completed", result.error)
        self.assertEqual((self.root / "counter").read_text(), "x")

    async def test_host_exit_stops_completed_invocation_background_process(self):
        """Normal chat completion retains a process until the owning Host exits."""
        command = python("import time; time.sleep(60)")
        provider = ScriptedProvider(LLMResponse(model="offline", finish_reason="tool_calls",
            output=(call(command=command, yield_seconds=0),)), answer())
        config = ExecutionToolsConfig(default_decision="allow")
        async with self.host(provider, workflow=agent_workflow(execution=config)) as host:
            await self.bind(host)
            result = await host.join(await host.execute("agent", AgentInput(message="run"), session_id="one"))
            self.assertEqual(result.status, "completed", result.error)
            manager = host.agent_context._workspaces.process_manager
            records = manager.records()
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0].handle.state, "running")
        self.assertEqual(records[0].handle.state, "terminated")

    async def test_invocation_cancel_stops_a_previously_yielded_process(self):
        """Native cancellation also stops processes whose launch Tool already returned."""
        command = python("import time; time.sleep(60)")
        provider = ScriptedProvider(
            LLMResponse(model="offline", finish_reason="tool_calls", output=(call(command=command, yield_seconds=0),)),
            LLMResponse(model="offline", finish_reason="tool_calls", output=(call("ask_user", questions=[{
                "id": "next", "type": "text", "text": "Continue?"}]),)))
        async with self.host(provider, workflow=agent_workflow(include_ask_user=True,
            execution=ExecutionToolsConfig(default_decision="allow"))) as host:
            await self.bind(host)
            ref = await host.execute("agent", AgentInput(message="run"), session_id="one")
            result = await host.join(ref)
            self.assertEqual(result.status, "waiting", result.error)
            records = host.agent_context._workspaces.process_manager.records()
            self.assertEqual(len(records), 1)
            await host.cancel(result.invocation_id)
            self.assertEqual(records[0].handle.state, "terminated")

    async def test_core_unload_does_not_dispose_the_session_process(self):
        """Core memory eviction does not close Host-owned process handles."""
        command = python("import time; time.sleep(60)")
        provider = ScriptedProvider(LLMResponse(model="offline", finish_reason="tool_calls",
            output=(call(command=command, yield_seconds=0),)), answer())
        async with self.host(provider, workflow=agent_workflow(execution=ExecutionToolsConfig(default_decision="allow"))) as host:
            await self.bind(host)
            ref = await host.execute("agent", AgentInput(message="run"), session_id="one")
            result = await host.join(ref)
            self.assertEqual(result.status, "completed", result.error)
            manager = host.agent_context._workspaces.process_manager
            records = manager.records()
            await host._runtime.app.aunload_session(host._runtime.resident["one"])
            host._runtime.forget("one")
            self.assertEqual(manager.records(), records)
            self.assertEqual(records[0].handle.state, "running")


class SQLiteHostExecutionTests(HostExecutionContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        return "sqlite+aiosqlite:///" + str(self.root / "runtime.db")


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL test database is not configured")
class PostgreSQLHostExecutionTests(HostExecutionContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()
