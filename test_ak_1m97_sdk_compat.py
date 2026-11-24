"""ak-1m97 (P0, 2026-11-24): agents must start on claude-agent-sdk 0.1.11.

Prod runs SDK 0.1.11, whose ClaudeAgentOptions has no ``tools`` field, so
``restricted_tool_options`` (ak-m0f6) raised TypeError in every agent and
the mail pipeline processed nothing since 13:43Z. Fix: feature-detect the
field. With it, unchanged; without it, the SAME ``--tools`` CLI flag goes
through ``extra_args`` (0.1.x maps {flag: value} → ``--flag value``),
plus ``disallowed_tools`` = every other known built-in. ak-4ded
containment is unchanged: only the allowed built-ins + ``--strict-mcp-config``.

The argv checks build the REAL command line with each SDK's own
SubprocessCLITransport._build_command, in a subprocess so each SDK
version is imported in isolation:
  * the installed (new) SDK, which has ``tools``;
  * a pre-``tools`` SDK from AK_PRE_TOOLS_SDK (a site-packages dir that
    contains claude_agent_sdk). It defaults to the 0.1.8 copy on the data
    disk; 0.1.11 is no longer on PyPI, so point this at prod's venv
    site-packages to run it against 0.1.11 itself.
(The classifier-failure reporting is a separate follow-up.)
"""
from __future__ import annotations

import dataclasses
import json
import os
import subprocess
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from utils import sdk_tools as T  # noqa: E402

PRE_TOOLS_SDK = os.environ.get("AK_PRE_TOOLS_SDK", "/home/opc/data/tmp/sdk_0.1.8")
ALLOWED = ["mcp__mail_processor_tools__insert_transaction", "mcp__srv__x"]

_ARGV_SCRIPT = r"""
import json, sys
repo, sdk = sys.argv[1], sys.argv[2]
if sdk:
    sys.path.insert(0, sdk)
sys.path.insert(0, repo)
for p in sys.argv[3:]:
    sys.path.append(p)
import dataclasses
import claude_agent_sdk
from claude_agent_sdk import ClaudeAgentOptions
from claude_agent_sdk._internal.transport.subprocess_cli import SubprocessCLITransport
from utils.sdk_tools import restricted_tool_options
fields = {f.name for f in dataclasses.fields(ClaudeAgentOptions)}
out = {"sdk_file": claude_agent_sdk.__file__, "has_tools_field": "tools" in fields}
for name, allowed in (("none", ["mcp__srv__x"]), ("web", ["mcp__srv__x", "WebSearch", "WebFetch"])):
    kw = restricted_tool_options(allowed)
    if "cli_path" in fields:
        kw["cli_path"] = "/bin/true"
    opts = ClaudeAgentOptions(**kw)
    t = SubprocessCLITransport("hi", opts)
    if "cli_path" not in fields:
        t._cli_path = "/bin/true"
    out[name] = t._build_command()
print(json.dumps(out))
"""


def _argv(sdk_dir):
    libs = [p for p in os.environ.get("PYTHONPATH", "").split(os.pathsep) if p]
    r = subprocess.run([sys.executable, "-I", "-c", _ARGV_SCRIPT, HERE, sdk_dir or "", *libs],
                       capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        raise AssertionError(r.stderr[-2000:])
    return json.loads(r.stdout.strip().splitlines()[-1])


def _flag_value(cmd, flag):
    i = cmd.index(flag)
    return cmd[i + 1]


class TestKwargs(unittest.TestCase):

    def test_new_sdk_uses_tools_field(self):
        @dataclasses.dataclass
        class New:
            allowed_tools: list = None
            tools: list = None
            disallowed_tools: list = None
            extra_args: dict = None
        kw = T.restricted_tool_options(ALLOWED + ["WebSearch"], {"x": "1"}, options_cls=New)
        self.assertEqual(kw["tools"], ["WebSearch"])
        self.assertNotIn("disallowed_tools", kw)
        self.assertEqual(kw["extra_args"], {"x": "1", "strict-mcp-config": None})
        New(**kw)

    def test_old_sdk_passes_the_same_flag_via_extra_args(self):
        @dataclasses.dataclass
        class Old:                         # 0.1.11's relevant shape: no `tools`
            allowed_tools: list = None
            disallowed_tools: list = None
            extra_args: dict = None
        kw = T.restricted_tool_options(ALLOWED, options_cls=Old)
        self.assertNotIn("tools", kw)
        self.assertEqual(kw["extra_args"], {"strict-mcp-config": None, "tools": ""})
        self.assertIn("Bash", kw["disallowed_tools"])
        self.assertIn("Read", kw["disallowed_tools"])
        Old(**kw)                          # constructs: no TypeError
        kw = T.restricted_tool_options(ALLOWED + ["WebSearch", "WebFetch"], options_cls=Old)
        self.assertEqual(kw["extra_args"]["tools"], "WebSearch,WebFetch")
        self.assertNotIn("WebSearch", kw["disallowed_tools"])
        self.assertNotIn("WebFetch", kw["disallowed_tools"])

    def test_feature_detection_not_version(self):
        src = open(os.path.join(HERE, "utils", "sdk_tools.py")).read()
        self.assertIn("dataclasses.fields(options_cls)", src)
        self.assertNotIn("__version__", src)

    def test_known_builtins_cover_the_dangerous_ones(self):
        for t in ("Bash", "Read", "Write", "Edit", "MultiEdit", "Glob", "Grep", "LS",
                  "WebFetch", "WebSearch", "Task", "NotebookEdit", "TodoWrite"):
            self.assertIn(t, T.KNOWN_BUILTIN_TOOLS)


class TestRealArgv(unittest.TestCase):

    def _assert_contained(self, out):
        none, web = out["none"], out["web"]
        self.assertIn("--strict-mcp-config", none)
        self.assertEqual(_flag_value(none, "--tools"), "")
        self.assertEqual(_flag_value(web, "--tools"), "WebSearch,WebFetch")
        self.assertIn("--allowedTools", none)

    def test_installed_sdk_argv(self):
        # Whatever SDK is installed (tools field or not), the CLI gets the
        # same containment flags.
        self._assert_contained(_argv(None))

    @unittest.skipUnless(os.path.isdir(os.path.join(PRE_TOOLS_SDK, "claude_agent_sdk")),
                         f"pre-tools SDK not at {PRE_TOOLS_SDK}")
    def test_pre_tools_sdk_argv(self):
        out = _argv(PRE_TOOLS_SDK)
        self.assertFalse(out["has_tools_field"], out["sdk_file"])
        self.assertIn(PRE_TOOLS_SDK, out["sdk_file"])
        self._assert_contained(out)
        dis = _flag_value(out["none"], "--disallowedTools")
        for t in ("Bash", "Read", "Write", "Edit", "WebFetch"):
            self.assertIn(t, dis.split(","))
        self.assertNotIn("WebSearch", _flag_value(out["web"], "--disallowedTools").split(","))


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
