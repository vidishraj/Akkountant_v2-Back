"""ak-m0f6 (2026-11-24): `allowed_tools` does NOT restrict agent tools.

Measured on claude-agent-sdk 0.1.36 / CLI 2.1.42 (prod's version) with a
synthetic canary file and no personal data:
  * permission_mode="bypassPermissions" + allowed_tools=[one MCP tool]
    (the shape most app agents used): the init tool list had 186 entries
    (Bash, Read, Edit, Write, Glob, Grep, ... + 162 CLI-config MCP tools)
    and the model used `Read` to return the canary file's contents.
  * + tools=[] + --strict-mcp-config: init tools == exactly the MCP tool;
    it stayed callable; the canary read was impossible.
  * tools=["WebSearch", "WebFetch"] + strict: exactly those + the MCP tool.

These tests pin the fix WITHOUT a live model:
  1. utils.sdk_tools.restricted_tool_options semantics;
  2. the EFFECTIVE CLI command the SDK builds from those options
     (`--tools`, `--strict-mcp-config`, `--allowedTools`), built by the
     SDK's own SubprocessCLITransport, offline;
  3. per-site: the built-in tools each app agent can be offered
     (none, except the investment chat agent + the Wealth Digest
     research run, which get WebSearch/WebFetch only);
  4. runtime proof: the init SystemMessage tool NAMES are logged
     (`agent_init ... tool_names=`) for every agent.
The repo-wide AST drift guard lives in
test_ak_vhc2_claude_agent_options_allowed_tools.py.
"""
from __future__ import annotations

import ast
import dataclasses
import logging
import os
import sys
import types
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from utils.sdk_tools import (  # noqa: E402
    STRICT_MCP_FLAG, builtin_tools, restricted_tool_options,
)

try:
    import claude_agent_sdk as sdk
    from claude_agent_sdk._internal.transport.subprocess_cli import (
        SubprocessCLITransport,
    )
    _HAVE_SDK = True
except ImportError:  # pragma: no cover
    _HAVE_SDK = False

MCP_A = "mcp__agent_tools__get_transactions"
MCP_B = "mcp__agent_tools__fetch_stock_news"


def _src(rel):
    with open(os.path.join(HERE, rel)) as f:
        return f.read()


class TestRestrictedToolOptions(unittest.TestCase):

    def test_builtins_are_the_non_mcp_names(self):
        self.assertEqual(builtin_tools([]), [])
        self.assertEqual(builtin_tools([MCP_A]), [])
        self.assertEqual(
            builtin_tools([MCP_A, "WebSearch", MCP_B, "WebFetch"]),
            ["WebSearch", "WebFetch"])

    def test_kwargs_shape(self):
        kw = restricted_tool_options([MCP_A, "WebSearch"])
        self.assertEqual(kw["allowed_tools"], [MCP_A, "WebSearch"])
        self.assertEqual(kw["tools"], ["WebSearch"])
        self.assertEqual(kw["extra_args"], {STRICT_MCP_FLAG: None})

    def test_extra_args_merged_not_dropped(self):
        kw = restricted_tool_options([], extra_args={"debug-to-stderr": None})
        self.assertEqual(kw["extra_args"],
                         {"debug-to-stderr": None, STRICT_MCP_FLAG: None})

    def test_caller_list_not_mutated(self):
        allowed = [MCP_A]
        extra = {"debug-to-stderr": None}
        restricted_tool_options(allowed, extra_args=extra)
        self.assertEqual(allowed, [MCP_A])
        self.assertEqual(extra, {"debug-to-stderr": None})


@unittest.skipUnless(_HAVE_SDK, "claude_agent_sdk not installed")
class TestEffectiveCliArgs(unittest.TestCase):
    """What the CLI is actually told, per the SDK's own builder."""

    def _cmd(self, **kw):
        o = sdk.ClaudeAgentOptions(cli_path="/bin/true", **kw)
        return SubprocessCLITransport(prompt="x", options=o)._build_command()

    @staticmethod
    def _arg(cmd, flag):
        return cmd[cmd.index(flag) + 1] if flag in cmd else None

    def test_old_shape_restricted_nothing(self):
        # The pre-fix shape: no --tools at all -> CLI default set.
        cmd = self._cmd(permission_mode="bypassPermissions",
                        allowed_tools=[])
        self.assertNotIn("--tools", cmd)
        self.assertNotIn("--allowedTools", cmd)  # [] isn't even emitted
        self.assertNotIn("--strict-mcp-config", cmd)

    def test_no_tools(self):
        cmd = self._cmd(permission_mode="bypassPermissions",
                        **restricted_tool_options([]))
        self.assertEqual(self._arg(cmd, "--tools"), "")
        self.assertIn("--strict-mcp-config", cmd)

    def test_mcp_only(self):
        cmd = self._cmd(permission_mode="bypassPermissions",
                        mcp_servers={"agent_tools": {
                            "type": "sdk", "name": "agent_tools",
                            "instance": None}},
                        **restricted_tool_options([MCP_A]))
        self.assertEqual(self._arg(cmd, "--tools"), "")
        self.assertEqual(self._arg(cmd, "--allowedTools"), MCP_A)
        self.assertIn("--strict-mcp-config", cmd)

    def test_web_plus_mcp(self):
        cmd = self._cmd(**restricted_tool_options(
            [MCP_A, "WebSearch", "WebFetch"]))
        self.assertEqual(self._arg(cmd, "--tools"), "WebSearch,WebFetch")
        self.assertIn("--strict-mcp-config", cmd)

    def test_debug_flag_kept(self):
        cmd = self._cmd(**restricted_tool_options(
            [], extra_args={"debug-to-stderr": None}))
        self.assertIn("--debug-to-stderr", cmd)
        self.assertIn("--strict-mcp-config", cmd)


class TestPerSiteBuiltinSets(unittest.TestCase):
    """Every site's restricted_tool_options(<arg>) argument, read from
    source by AST; assert which built-ins it can resolve to."""

    SITES = {
        "services/jobEmailService.py": 1,
        "services/mailProcessorService.py": 4,
        "services/cronAgent.py": 1,
        "utils/web_extract.py": 1,
        "services/agentService.py": 1,
        "services/tasks/WealthDigestTask.py": 2,
    }

    def _calls(self, rel):
        out = []
        for node in ast.walk(ast.parse(_src(rel))):
            if (isinstance(node, ast.Call)
                    and getattr(node.func, "id", None) == "restricted_tool_options"):
                out.append(node)
        return out

    def test_site_inventory(self):
        for rel, n in self.SITES.items():
            self.assertEqual(len(self._calls(rel)), n, rel)

    def test_literal_and_mcp_only_sites_have_no_builtins(self):
        for rel in ("services/jobEmailService.py",
                    "services/mailProcessorService.py",
                    "services/cronAgent.py", "utils/web_extract.py"):
            for call in self._calls(rel):
                arg = call.args[0]
                if isinstance(arg, ast.List):
                    # [] literal
                    self.assertEqual(arg.elts, [], rel)
                else:
                    # [f"mcp__{...}__{...}" for t in ...] — every element
                    # is an mcp__ f-string, so builtin_tools() is empty.
                    self.assertIsInstance(arg, ast.ListComp, rel)
                    elt = arg.elt
                    self.assertIsInstance(elt, ast.JoinedStr, rel)
                    self.assertTrue(
                        isinstance(elt.values[0], ast.Constant)
                        and elt.values[0].value.startswith("mcp__"), rel)

    def test_chat_agent_builtins_are_web_only_for_investment(self):
        # AST-lift compute_allowed_tools (pure) without the Flask chain.
        src = _src("services/agentService.py")
        tree = ast.parse(src)
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                  and n.name == "compute_allowed_tools")
        ns = {"_WEB_BUILT_IN_TOOLS": ("WebSearch", "WebFetch"),
              "MCP_SERVER_NAME": "agent_tools",
              "READ_ATTACHMENT_TOOL_NAME": "read_attachment"}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), "<lift>", "exec"), ns)
        cat = ns["compute_allowed_tools"]
        for agent in ("transaction", "freelance"):
            for att in (False, True):
                self.assertEqual(builtin_tools(cat(agent, [MCP_A], att)), [])
        for att in (False, True):
            self.assertEqual(
                sorted(builtin_tools(cat("investment", [MCP_A], att))),
                ["WebFetch", "WebSearch"])
        # ...and the chat options derive tools= from that same list.
        self.assertIn("**restricted_tool_options(\n                    mcp_tool_names,", src)

    def test_digest_builtins(self):
        src = _src("services/tasks/WealthDigestTask.py")
        tree = ast.parse(src)
        allowed = None
        for n in tree.body:
            if (isinstance(n, ast.Assign) and len(n.targets) == 1
                    and getattr(n.targets[0], "id", None) == "_DIGEST_ALLOWED_TOOLS"):
                names = []
                for e in n.value.elts:
                    if isinstance(e, ast.Constant):
                        names.append(e.value)
                    else:  # f"mcp__{_MCP_SERVER}__x"
                        names.append("mcp__" + "x")
                allowed = names
        self.assertEqual(sorted(builtin_tools(allowed)), ["WebFetch", "WebSearch"])
        args = [ast.unparse(c.args[0]) for c in
                (n for n in ast.walk(tree) if isinstance(n, ast.Call)
                 and getattr(n.func, "id", None) == "restricted_tool_options")]
        self.assertEqual(sorted(args), ["[]", "_DIGEST_ALLOWED_TOOLS"])


@unittest.skipUnless(_HAVE_SDK, "claude_agent_sdk not installed")
class TestInitToolNamesLogged(unittest.TestCase):
    """Runtime proof of the effective set: the CLI's init SystemMessage
    tool NAMES are logged for every agent (names only)."""

    def setUp(self):
        stubs = {
            "flask": types.ModuleType("flask"),
            "utils.logger": types.ModuleType("utils.logger"),
        }
        stubs["flask"].jsonify = lambda *a, **k: None

        class _L:
            def __init__(self, name="x"):
                self._l = logging.getLogger(f"ak_m0f6.{name}")

            def get_logger(self):
                return self._l
        stubs["utils.logger"].Logger = _L
        self._p = mock.patch.dict(sys.modules, stubs)
        self._p.start()
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "ak_m0f6_runner", os.path.join(HERE, "utils", "sdk_runner.py"))
        self.r = importlib.util.module_from_spec(spec)
        sys.modules["ak_m0f6_runner"] = self.r
        spec.loader.exec_module(self.r)

    def tearDown(self):
        self._p.stop()

    def _init(self, tools):
        fields = {f.name for f in dataclasses.fields(sdk.SystemMessage)}
        kw = {"subtype": "init", "data": {"tools": tools, "model": "m"}}
        return sdk.SystemMessage(**{k: v for k, v in kw.items() if k in fields})

    def test_log_init_tools_helper(self):
        with self.assertLogs(self.r._logger, level="INFO") as cm:
            names = self.r.log_init_tools("chat.transaction",
                                          self._init([MCP_A, "StructuredOutput"]))
        self.assertEqual(names, [MCP_A, "StructuredOutput"])
        self.assertIn(
            f"agent_init agent=chat.transaction tools_offered=2 "
            f"tool_names=StructuredOutput,{MCP_A}", "\n".join(cm.output))

    def test_non_init_messages_ignored(self):
        self.assertIsNone(self.r.log_init_tools("a", object()))

    def test_run_query_collect_logs_by_default(self):
        import anyio
        init = self._init([MCP_A])

        async def q(*, prompt, options):
            yield init
        opts = types.SimpleNamespace(model="m", stderr=None)
        with mock.patch.object(self.r, "sdk_query", q), \
                self.assertLogs(self.r._logger, level="INFO") as cm:
            res = anyio.run(lambda: self.r.run_query_collect(
                agent="mail.classify", options=opts, prompt="x"))
        self.assertEqual(res.init_tools, [MCP_A])
        self.assertIn("agent_init agent=mail.classify tools_offered=1",
                      "\n".join(cm.output))

    def test_chat_agent_loop_logs_init(self):
        src = _src("services/agentService.py")
        loop = src[src.index("async for message in query(prompt=make_prompt(), options=options):"):]
        self.assertIn('log_init_tools(f"chat.{agent_type}", message)', loop[:400])


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
