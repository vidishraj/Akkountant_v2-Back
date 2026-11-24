"""ak-lwed (2026-11-24): Wealth Digest returned empty structured_output
on every attempt -> job Failed nightly.

Root cause (measured with live SDK probes, synthetic data): the
structured result is delivered by the model CALLING the CLI's
StructuredOutput tool, and that call costs a turn, as do the CLI's own
ToolSearch / ReadMcpResourceTool lookups. With max_turns=6 the research
loop spent every turn on tools (prod: tool_calls=6 on all 4 attempts).
On prod's claude-agent-sdk 0.1.36 the run then ends with
ResultMessage(subtype="error_max_turns", is_error=False,
structured_output=None), so the agent_run line read status=ok.

These tests reproduce that exact "agent ok, no structured_output" shape
through a fake SDK stream / scripted SdkRunResults and pin the fix:
  * utils.sdk_runner surfaces ResultMessage.subtype;
  * the digest has a real turn budget;
  * after a turn exhaustion (and always on the final attempt) the
    digest falls back to a no-tools synthesis run so it still ships;
  * the scheduler emits job_failure_alert at failures >= 3.

Heavy Flask/DB deps are stubbed only inside these tests via
mock.patch.dict(sys.modules), so nothing leaks into other test modules.
Requires claude_agent_sdk + anyio (both app deps); skipped otherwise.
"""
from __future__ import annotations

import dataclasses
import importlib.util
import json
import logging
import os
import sys
import types
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

try:
    import anyio  # noqa: F401
    import claude_agent_sdk as sdk
    _HAVE_SDK = True
except ImportError:  # pragma: no cover
    _HAVE_SDK = False


class _FakeLogger:
    def __init__(self, name="x"):
        self._l = logging.getLogger(f"ak_lwed_test.{name}")

    def get_logger(self):
        return self._l


def _stub_modules():
    """Minimal stand-ins for the Flask/DB import chain."""
    def mod(name, **attrs):
        m = types.ModuleType(name)
        for k, v in attrs.items():
            setattr(m, k, v)
        return m

    class _BaseTask:
        def __init__(self, *a, **k):
            pass

    return {
        "flask": mod("flask", jsonify=lambda *a, **k: None),
        "utils.logger": mod("utils.logger", Logger=_FakeLogger),
        "enums.EPGEnum": mod("enums.EPGEnum", EPGEnum=object),
        "enums.MsnEnum": mod("enums.MsnEnum", MSNENUM=object),
        "services.agentConversationService": mod(
            "services.agentConversationService",
            AgentConversationService=object,
            WEALTH_DIGEST_TITLE="Wealth Digest",
        ),
        "services.tasks.baseTask": mod(
            "services.tasks.baseTask", BaseTask=_BaseTask),
    }


def _load(rel_path, name):
    spec = importlib.util.spec_from_file_location(
        name, os.path.join(HERE, rel_path))
    m = importlib.util.module_from_spec(spec)
    # dataclasses resolves annotations via sys.modules[__module__];
    # callers run this inside mock.patch.dict(sys.modules), so the
    # registration is undone afterwards.
    sys.modules[name] = m
    spec.loader.exec_module(m)
    return m


def _make(cls, **kw):
    """Build an SDK dataclass across versions: fill required fields we
    don't care about with None."""
    fields = {f.name: f for f in dataclasses.fields(cls)}
    args = {}
    for n, f in fields.items():
        if n in kw:
            args[n] = kw[n]
        elif (f.default is dataclasses.MISSING
              and f.default_factory is dataclasses.MISSING):
            args[n] = None
    return cls(**args)


@unittest.skipUnless(_HAVE_SDK, "claude_agent_sdk/anyio not installed")
class TestSdkRunnerSurfacesSubtype(unittest.TestCase):
    """The prod failure shape: tools used up the budget, ResultMessage
    says error_max_turns with is_error=False and no structured output."""

    def setUp(self):
        self._p = mock.patch.dict(sys.modules, _stub_modules())
        self._p.start()
        self.runner = _load("utils/sdk_runner.py", "ak_lwed_sdk_runner")

    def tearDown(self):
        self._p.stop()

    def _fake_query(self, n_tools):
        async def q(*, prompt, options):
            for i in range(n_tools):
                yield _make(sdk.AssistantMessage, model="m", content=[
                    _make(sdk.ToolUseBlock, id=f"t{i}",
                          name="mcp__agent_tools__fetch_stock_news",
                          input={}),
                ])
            yield _make(sdk.ResultMessage, subtype="error_max_turns",
                        duration_ms=1, duration_api_ms=1, is_error=False,
                        num_turns=n_tools + 1, session_id="s",
                        structured_output=None, result=None)
        return q

    def test_agent_ok_but_no_structured_output_is_visible(self):
        opts = types.SimpleNamespace(model="m", stderr=None)
        with mock.patch.object(self.runner, "sdk_query", self._fake_query(6)), \
                self.assertLogs(self.runner._logger, level="INFO") as cm:
            res = anyio.run(lambda: self.runner.run_query_collect(
                agent="wealth_digest", options=opts, prompt="x"))
        # The exact prod shape: no error, no structured output, 6 tools.
        self.assertIsNone(res.error)
        self.assertIsNone(res.structured_output)
        self.assertEqual(res.tool_calls, 6)
        # ...and now the cause is no longer hidden.
        self.assertEqual(res.subtype, "error_max_turns")
        line = "\n".join(cm.output)
        self.assertIn("status=ok", line)
        self.assertIn("result_subtype=error_max_turns", line)

    def test_init_tool_names_captured_and_logged_on_request(self):
        async def q(*, prompt, options):
            yield _make(sdk.SystemMessage, subtype="init",
                        data={"tools": ["StructuredOutput"], "model": "m"})
            yield _make(sdk.ResultMessage, subtype="success", duration_ms=1,
                        duration_api_ms=1, is_error=False, num_turns=1,
                        session_id="s", structured_output={"a": 1})
        for flag in (True, False):
            opts = types.SimpleNamespace(model="m", stderr=None)
            with mock.patch.object(self.runner, "sdk_query", q), \
                    self.assertLogs(self.runner._logger, level="INFO") as cm:
                res = anyio.run(lambda: self.runner.run_query_collect(
                    agent="wealth_digest", options=opts, prompt="x",
                    log_init_tools=flag))
            self.assertEqual(res.init_tools, ["StructuredOutput"])
            logged = "\n".join(cm.output)
            if flag:
                self.assertIn("agent_init agent=wealth_digest tools_offered=1 "
                              "tool_names=StructuredOutput", logged)
            else:
                self.assertNotIn("agent_init", logged)

    def test_success_subtype_not_logged(self):
        async def q(*, prompt, options):
            yield _make(sdk.ResultMessage, subtype="success", duration_ms=1,
                        duration_api_ms=1, is_error=False, num_turns=1,
                        session_id="s", structured_output={"a": 1})
        opts = types.SimpleNamespace(model="m", stderr=None)
        with mock.patch.object(self.runner, "sdk_query", q), \
                self.assertLogs(self.runner._logger, level="INFO") as cm:
            res = anyio.run(lambda: self.runner.run_query_collect(
                agent="a", options=opts, prompt="x"))
        self.assertEqual(res.subtype, "success")
        self.assertNotIn("result_subtype", "\n".join(cm.output))


@unittest.skipUnless(_HAVE_SDK, "claude_agent_sdk/anyio not installed")
class TestDigestTurnBudgetAndFallback(unittest.TestCase):

    def setUp(self):
        self._p = mock.patch.dict(sys.modules, _stub_modules())
        self._p.start()
        # Fresh copies so the stubbed chain is what they import.
        runner = _load("utils/sdk_runner.py", "utils.sdk_runner")
        sys.modules["utils.sdk_runner"] = runner
        self.SdkRunResult = runner.SdkRunResult
        self.m = _load("services/tasks/WealthDigestTask.py",
                       "ak_lwed_wealth_digest")
        self.calls = []
        self.log_flags = []
        self.fake_self = types.SimpleNamespace(
            logger=logging.getLogger("ak_lwed_test.digest"),
            # Same shape create_sdk_mcp_server returns (type=sdk).
            _build_digest_mcp_server=lambda uid: (
                {"type": "sdk", "name": "agent_tools", "instance": None}, []),
        )
        self._sleep = mock.patch.object(self.m.time, "sleep", lambda s: None)
        self._sleep.start()

    def tearDown(self):
        self._sleep.stop()
        self._p.stop()

    def _result(self, structured=None, subtype="success", tools=0,
                init_tools=None):
        return self.SdkRunResult(
            text="", structured_output=structured, tool_calls=tools,
            turns=tools + 1, latency_ms=1, error=None, error_class=None,
            subtype=subtype, init_tools=init_tools)

    @staticmethod
    def _cli_args(options):
        """The EFFECTIVE CLI command the SDK would spawn for these
        options (built by the SDK's own transport, offline)."""
        from claude_agent_sdk._internal.transport.subprocess_cli import (
            SubprocessCLITransport,
        )
        o = dataclasses.replace(options, cli_path="/bin/true")
        return SubprocessCLITransport(prompt="x", options=o)._build_command()

    def _assert_tool_free(self, options):
        cmd = self._cli_args(options)
        i = cmd.index("--tools")
        self.assertEqual(cmd[i + 1], "")             # no built-ins
        self.assertIn("--strict-mcp-config", cmd)     # no external MCP
        self.assertNotIn("--allowedTools", cmd)
        self.assertNotIn("--mcp-config", cmd)         # no MCP servers
        self.assertIn("--json-schema", cmd)           # StructuredOutput kept

    def _script(self, outcomes):
        """Patch run_query_collect to return/raise `outcomes` in order
        and record each call's options."""
        it = iter(outcomes)

        async def fake(*, agent, options, prompt, user_id=None,
                       log_init_tools=False):
            self.calls.append(options)
            self.log_flags.append(log_init_tools)
            o = next(it)
            if isinstance(o, BaseException):
                raise o
            return o
        return mock.patch.object(self.m, "run_query_collect", fake)

    def _digest(self, news=None):
        return {"text": "body", "actions": [], "watch_items": [],
                "news": news if news is not None else [{"title": "n"}]}

    def _invoke(self):
        return self.m.WealthDigestTask._invoke_agent(
            self.fake_self, {"k": "v"}, user_id="u")

    def test_budget_leaves_room_for_structured_output(self):
        m = self.m
        # research cap + ToolSearch/loader overhead + StructuredOutput
        # + at least one schema retry.
        self.assertGreaterEqual(
            m._DIGEST_MAX_TURNS, m._DIGEST_RESEARCH_TOOL_BUDGET + 4)
        self.assertGreater(m._DIGEST_MAX_TURNS, 6)  # the prod value that failed
        prompt = m._build_system_prompt(*m._make_user_data_tags())
        self.assertIn(f"at most {m._DIGEST_RESEARCH_TOOL_BUDGET} research "
                      f"tool calls", prompt)

    def test_research_attempt_uses_full_budget_and_tools(self):
        with self._script([self._result(self._digest(), tools=5)]):
            out = json.loads(self._invoke())
        self.assertEqual(len(self.calls), 1)
        opts = self.calls[0]
        self.assertEqual(self.log_flags, [True])
        self.assertEqual(opts.max_turns, self.m._DIGEST_MAX_TURNS)
        cmd = self._cli_args(opts)
        self.assertIn("--allowedTools", cmd)
        # ak-m0f6: research is restricted too: exactly the two web
        # built-ins + the code-declared MCP wrappers.
        self.assertEqual(cmd[cmd.index("--tools") + 1], "WebSearch,WebFetch")
        self.assertIn("--strict-mcp-config", cmd)
        self.assertIn("--mcp-config", cmd)
        self.assertTrue(opts.mcp_servers)
        self.assertEqual(opts.allowed_tools, self.m._DIGEST_ALLOWED_TOOLS)
        self.assertEqual(out["news"], [{"title": "n"}])  # grounded news kept
        self.assertTrue(out["text"].startswith(self.m._HEADER))
        self.assertNotIn(self.m._NO_LIVE_NEWS_NOTE, out["text"])

    def test_max_turns_exhaustion_falls_back_to_synthesis_only(self):
        # Attempt 1 reproduces prod exactly; attempt 2 must not repeat
        # the research run.
        with self._script([
            self._result(None, subtype="error_max_turns", tools=6),
            self._result(self._digest(news=[{"title": "invented"}])),
        ]):
            out = json.loads(self._invoke())
        self.assertEqual(len(self.calls), 2)
        synth = self.calls[1]
        self.assertEqual(synth.max_turns, self.m._DIGEST_SYNTHESIS_MAX_TURNS)
        self.assertEqual(synth.tools, [])
        self.assertFalse(synth.mcp_servers)
        self._assert_tool_free(synth)
        self.assertIn("synthesis only", synth.system_prompt)
        self.assertEqual(out["news"], [])  # ungrounded news never persisted
        self.assertTrue(out["text"].endswith(self.m._NO_LIVE_NEWS_NOTE))

    def test_newer_sdk_raise_on_turn_exhaustion_is_not_terminal(self):
        # claude-agent-sdk 0.2.x raises instead of returning a subtype.
        err = RuntimeError("Claude Code returned an error result: Reached "
                           "maximum number of turns (18)")
        with self._script([err, self._result(self._digest())]):
            out = json.loads(self._invoke())
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(self.calls[1].max_turns,
                         self.m._DIGEST_SYNTHESIS_MAX_TURNS)
        self.assertEqual(out["news"], [])

    def test_research_timeout_routes_to_synthesis(self):
        import asyncio
        with self._script([asyncio.TimeoutError(),
                           self._result(self._digest())]):
            out = json.loads(self._invoke())
        self.assertEqual(len(self.calls), 2)
        self._assert_tool_free(self.calls[1])
        self.assertEqual(out["news"], [])

    def test_unexpected_offered_tools_in_synthesis_are_flagged(self):
        with self._script([
            self._result(None, subtype="error_max_turns", tools=6),
            self._result(self._digest(),
                         init_tools=["StructuredOutput", "Bash"]),
        ]), self.assertLogs(self.fake_self.logger, level="WARNING") as cm:
            self._invoke()
        self.assertIn("unexpected tools: Bash", "\n".join(cm.output))

    def test_clean_synthesis_offer_not_flagged(self):
        with self._script([
            self._result(None, subtype="error_max_turns", tools=6),
            self._result(self._digest(), init_tools=["StructuredOutput"]),
        ]), self.assertLogs(self.fake_self.logger, level="WARNING") as cm:
            self._invoke()
        self.assertNotIn("unexpected tools", "\n".join(cm.output))

    def test_final_attempt_is_always_synthesis_only(self):
        n = self.m._SDK_MAX_RETRIES + 1
        outcomes = [self._result(None, subtype="success", tools=2)
                    for _ in range(n - 1)] + [self._result(self._digest())]
        with self._script(outcomes):
            out = json.loads(self._invoke())
        self.assertEqual(len(self.calls), n)
        for o in self.calls[:-1]:
            self.assertEqual(o.max_turns, self.m._DIGEST_MAX_TURNS)
        self.assertEqual(self.calls[-1].max_turns,
                         self.m._DIGEST_SYNTHESIS_MAX_TURNS)
        self.assertEqual(out["news"], [])

    def test_still_fails_loudly_if_synthesis_also_fails(self):
        n = self.m._SDK_MAX_RETRIES + 1
        with self._script([self._result(None, subtype="error_max_turns",
                                        tools=6)] * n):
            with self.assertRaises(RuntimeError) as cm:
                self._invoke()
        self.assertIn("exhausted", str(cm.exception))
        self.assertIn("error_max_turns", str(cm.exception))


class TestSchedulerFailureAlert(unittest.TestCase):
    """A recurring chain that is failing must be loud at failures >= 3,
    long before rescheduling silently stops at 10."""

    def test_alert_wired(self):
        with open(os.path.join(HERE, "services", "tasks", "scheduler.py")) as f:
            src = f.read()
        self.assertIn("JOB_FAILURE_ALERT_THRESHOLD = 3", src)
        block = src[src.index("job.failures = min(job.failures + 1, 10)"):]
        block = block[:block.index("else:\n                    job.failures = 0")]
        self.assertIn("job.failures >= JOB_FAILURE_ALERT_THRESHOLD", block)
        self.assertIn("self.logger.error(", block)
        self.assertIn("job_failure_alert job={job.title}", block)
        self.assertNotIn("job.result", block)  # counts only, no content


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
