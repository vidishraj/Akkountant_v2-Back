"""ak-dixt SECURITY C-4 (2026-11-23): regression tests for the
server-signed destructive-confirm token system that replaces the
pre-ak-dixt client-asserted `confirmed_tools: [str]` list.

Vulnerability class (pre-ak-dixt):
  * controllers/agentEP.py accepted `confirmed_tools: [str]` as
    the destructive confirmation payload. No server token, no args
    binding, no expiry, no one-time-use. A compromised client (or
    prompt-injected agent output causing a tool call) could emit
    ANY destructive tool call with `confirmed_tools: ["delete_
    customer"]` and bypass the human-confirmation step entirely.
  * agentService.py :1197 + :1231 both gated via `_tn in confirmed
    _tools` — purely client-asserted.
  * update_invoice was NOT in DESTRUCTIVE_TOOLS at all — ungated
    mutation path.

Fix (ak-dixt):
  * utils/confirm_tokens.py: issue_confirm_token() mints an HMAC-
    signed opaque token binding (tool_name, args_hash, user_id,
    exp, nonce). verify_confirm_token() validates signature +
    binding + expiry + one-time-use. Constant-time compare.
  * agentService._build_sdk_tools: _is_call_confirmed(tool, args)
    helper tries the hardened token list first; falls through to
    the legacy name-list with a WARN log (dual-accept migration
    window per ak-x74g precedent).
  * confirm SSE event payload carries `confirm_id` (the minted
    token) for FE to echo on the retry turn.
  * update_invoice added to DESTRUCTIVE_TOOLS.
  * controllers/agentEP.py parses `confirmed_calls` (new field,
    [token]) alongside legacy `confirmed_tools` ([name]).

Fail-closed invariants:
  * Missing AK_CONFIRM_SECRET: issue returns None, verify returns
    False. Legacy name-list dual-accept keeps UX alive but server-
    signed path is dormant.
  * Any tamper (signature, args, user, tool, expiry): False.
  * Replay within TTL: in-memory consumed-set rejects second
    verify. (Multi-worker caveat: residual risk mitigated by 300s
    TTL — follow-up bead files DB-backed durable dedup if we go
    multi-user.)

Pin-downs:
  (1-7)   issue_confirm_token basic shape + fail-closed on
          missing secret + None/empty validation.
  (8-14)  verify_confirm_token accept/reject matrix: good token,
          tampered signature, wrong tool, wrong args, wrong user,
          expired, malformed.
  (15)    Replay rejection (consumed-set).
  (16)    Constant-time signature compare (no raise on
          wrong-length sig).
  (17)    Args ordering independence — same args dict in any key
          order produces the same args_hash.
  (18)    update_invoice is in DESTRUCTIVE_TOOLS (source-grep).
  (19-23) Source-grep drift guards on the agentService.py +
          agentEP.py gate shape.
  (24-25) Legacy dual-accept WARN-log path source-grep.
"""
from __future__ import annotations

import ast
import hashlib
import json
import os
import re
import sys
import time
import unittest
from unittest import mock


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _read_source(rel_path):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


# ─── Pure-unit: utils/confirm_tokens.py ──────────────────────
#
# This module imports stdlib-only (hmac + hashlib + base64 + json
# + os + secrets + threading + time + collections). Safe to import
# directly (no anyio / Firebase / Flask chain).


class TestIssueConfirmToken(unittest.TestCase):

    def setUp(self):
        from utils.confirm_tokens import _reset_consumed_for_tests
        _reset_consumed_for_tests()

    # (1) Missing secret → None + WARN.
    def test_missing_secret_returns_none(self):
        from utils import confirm_tokens as ct
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("AK_CONFIRM_SECRET", None)
            self.assertIsNone(ct.issue_confirm_token(
                "delete_invoice", {"id": "x"}, user_id="U",
            ))

    # (2) Present secret → non-None token with the `b64.hex` shape.
    def test_valid_mint_shape(self):
        from utils import confirm_tokens as ct
        with mock.patch.dict(
            os.environ,
            {"AK_CONFIRM_SECRET": "testsecret-very-random"},
            clear=False,
        ):
            tok = ct.issue_confirm_token(
                "delete_invoice", {"id": "x"}, user_id="U",
            )
        self.assertIsNotNone(tok)
        self.assertIn(".", tok)
        parts = tok.rsplit(".", 1)
        self.assertEqual(len(parts), 2)
        # Signature is hex sha256 → 64 chars.
        self.assertEqual(len(parts[1]), 64)
        self.assertRegex(parts[1], r"^[0-9a-f]{64}$")

    # (3) Non-str tool_name rejected.
    def test_non_str_tool_rejected(self):
        from utils import confirm_tokens as ct
        with mock.patch.dict(
            os.environ,
            {"AK_CONFIRM_SECRET": "s"},
            clear=False,
        ):
            self.assertIsNone(ct.issue_confirm_token(123, {}, "U"))
            self.assertIsNone(ct.issue_confirm_token("", {}, "U"))

    # (4) Non-str user_id rejected.
    def test_non_str_user_rejected(self):
        from utils import confirm_tokens as ct
        with mock.patch.dict(
            os.environ,
            {"AK_CONFIRM_SECRET": "s"},
            clear=False,
        ):
            self.assertIsNone(
                ct.issue_confirm_token("t", {}, user_id=None),
            )
            self.assertIsNone(
                ct.issue_confirm_token("t", {}, user_id=""),
            )

    # (5) None args hashes to a stable value distinct from empty dict.
    def test_none_args_distinct_from_empty_args(self):
        from utils import confirm_tokens as ct
        with mock.patch.dict(
            os.environ,
            {"AK_CONFIRM_SECRET": "s"},
            clear=False,
        ):
            tok_none = ct.issue_confirm_token("t", None, "U")
            tok_empty = ct.issue_confirm_token("t", {}, "U")
        # Decode both payloads and compare args_hash.
        def _unpack(tok):
            import base64
            b64, _sig = tok.rsplit(".", 1)
            pad = "=" * (-len(b64) % 4)
            return json.loads(base64.urlsafe_b64decode(b64 + pad))
        h_none = _unpack(tok_none)["args_hash"]
        h_empty = _unpack(tok_empty)["args_hash"]
        self.assertNotEqual(
            h_none, h_empty,
            "None args must hash distinctly from empty-dict args; "
            "otherwise a confirm for one binds the other",
        )

    # (6) Same (tool, args, user) + two mints → different nonces
    # → different signatures. Prevents trivial token-collision
    # replay.
    def test_distinct_nonces_per_mint(self):
        from utils import confirm_tokens as ct
        with mock.patch.dict(
            os.environ,
            {"AK_CONFIRM_SECRET": "s"},
            clear=False,
        ):
            tok1 = ct.issue_confirm_token("t", {"a": 1}, "U")
            tok2 = ct.issue_confirm_token("t", {"a": 1}, "U")
        self.assertNotEqual(tok1, tok2)

    # (7) Non-positive TTL falls back to default (300s).
    def test_bad_ttl_falls_back_to_default(self):
        from utils import confirm_tokens as ct
        with mock.patch.dict(
            os.environ,
            {"AK_CONFIRM_SECRET": "s"},
            clear=False,
        ):
            tok = ct.issue_confirm_token("t", {}, "U", ttl_s=-5)
        self.assertIsNotNone(tok)


class TestVerifyConfirmToken(unittest.TestCase):

    def setUp(self):
        from utils.confirm_tokens import _reset_consumed_for_tests
        _reset_consumed_for_tests()
        self._env = mock.patch.dict(
            os.environ,
            {"AK_CONFIRM_SECRET": "testsecret-verify"},
            clear=False,
        )
        self._env.start()

    def tearDown(self):
        self._env.stop()

    # (8) Round-trip: issue → verify succeeds.
    def test_round_trip_ok(self):
        from utils import confirm_tokens as ct
        tok = ct.issue_confirm_token(
            "delete_invoice", {"id": "inv-1"}, "U-abc",
        )
        self.assertTrue(
            ct.verify_confirm_token(
                tok, "delete_invoice", {"id": "inv-1"}, "U-abc",
            ),
        )

    # (9) Tampered signature (appended/mutated hex) → False.
    def test_tampered_signature_rejected(self):
        from utils import confirm_tokens as ct
        tok = ct.issue_confirm_token("t", {"a": 1}, "U")
        b64, sig = tok.rsplit(".", 1)
        bad_sig = "f" * 64 if sig != "f" * 64 else "0" * 64
        bad_tok = f"{b64}.{bad_sig}"
        self.assertFalse(
            ct.verify_confirm_token(bad_tok, "t", {"a": 1}, "U"),
        )

    # (10) Wrong tool → False.
    def test_wrong_tool_rejected(self):
        from utils import confirm_tokens as ct
        tok = ct.issue_confirm_token("delete_invoice", {}, "U")
        self.assertFalse(
            ct.verify_confirm_token(
                tok, "delete_customer", {}, "U",
            ),
            "ak-dixt: a token issued for delete_invoice must NOT "
            "verify for delete_customer — binds the confirmation "
            "to the tool",
        )

    # (11) Wrong args → False.
    def test_wrong_args_rejected(self):
        from utils import confirm_tokens as ct
        tok = ct.issue_confirm_token("delete_invoice", {"id": "A"}, "U")
        self.assertFalse(
            ct.verify_confirm_token(
                tok, "delete_invoice", {"id": "B"}, "U",
            ),
            "ak-dixt: a token issued for id=A must NOT verify for "
            "id=B — binds the confirmation to args",
        )

    # (12) Wrong user → False.
    def test_wrong_user_rejected(self):
        from utils import confirm_tokens as ct
        tok = ct.issue_confirm_token("t", {}, "U-1")
        self.assertFalse(
            ct.verify_confirm_token(tok, "t", {}, "U-2"),
            "ak-dixt: a token issued for U-1 must NOT verify for "
            "U-2 — binds the confirmation to user",
        )

    # (13) Expired token → False.
    def test_expired_rejected(self):
        from utils import confirm_tokens as ct
        with mock.patch("utils.confirm_tokens.time.time") as mt:
            mt.return_value = 1_000_000
            tok = ct.issue_confirm_token("t", {}, "U", ttl_s=60)
            mt.return_value = 1_000_000 + 120  # +120s > TTL
            self.assertFalse(
                ct.verify_confirm_token(tok, "t", {}, "U"),
            )

    # (14) Malformed token shapes → False (no raise).
    def test_malformed_tokens_rejected(self):
        from utils import confirm_tokens as ct
        for bad in [
            None, "", "no-dot", "a.b.c", ".", "x.",
            ".x", 12345,
            ["token"], {"token": "x"},
        ]:
            self.assertFalse(
                ct.verify_confirm_token(bad, "t", {}, "U"),
                f"ak-dixt: malformed token {bad!r} must reject "
                "without raising",
            )

    # (15) Replay: same token verified twice → second False.
    def test_replay_rejected(self):
        from utils import confirm_tokens as ct
        tok = ct.issue_confirm_token("t", {"a": 1}, "U")
        self.assertTrue(
            ct.verify_confirm_token(tok, "t", {"a": 1}, "U"),
        )
        self.assertFalse(
            ct.verify_confirm_token(tok, "t", {"a": 1}, "U"),
            "ak-dixt: one-time-use — a verified token must NOT "
            "verify again within TTL (replay rejection)",
        )

    # (15b) ak-dixt reviewer MAJOR (2026-11-23): base64url malle-
    # ability replay regression. When payload byte-length is not a
    # multiple of 3, the trailing b64url char has unused bits, so
    # multiple distinct token STRINGS decode to the SAME payload
    # bytes + HMAC-verify under the same signature.
    #
    # Pre-fix (sha256(token)-keyed dedup): the two distinct token
    # strings hash to two DIFFERENT dedup keys, so the second
    # verify bypasses one-time-use. Reviewer proved this e2e.
    #
    # Post-fix (payload["nonce"]-keyed dedup): both tokens decode
    # to the same payload, same nonce, same dedup key → second
    # verify fails as expected.
    def test_replay_via_base64url_reencode_rejected(self):
        from utils import confirm_tokens as ct
        tok = ct.issue_confirm_token(
            "delete_invoice", {"id": "A"}, "U-replay-reencode",
        )
        b64_payload, sig = tok.rsplit(".", 1)
        original_bytes = ct._b64url_decode(b64_payload)
        if len(original_bytes) % 3 == 0:
            self.skipTest(
                "payload lands on 3-byte boundary — no unused "
                "bits in trailing b64 char to exploit on this "
                "fixture; the invariant still holds, just no "
                "attack to demonstrate"
            )
        # Find a last-char variant that is DIFFERENT but decodes
        # to the SAME payload bytes (= same HMAC sig).
        alphabet = (
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz"
            "0123456789-_"
        )
        last = b64_payload[-1]
        mutated_tok = None
        for c in alphabet:
            if c == last:
                continue
            candidate_b64 = b64_payload[:-1] + c
            try:
                candidate_bytes = ct._b64url_decode(candidate_b64)
            except Exception:
                continue
            if candidate_bytes == original_bytes:
                mutated_tok = f"{candidate_b64}.{sig}"
                break
        self.assertIsNotNone(
            mutated_tok,
            "could not find a malleable b64url variant — the "
            "payload-length / trailing-bit arithmetic changed",
        )
        self.assertNotEqual(
            mutated_tok, tok,
            "mutated token must be a distinct string from the "
            "original — attack-shape precondition",
        )
        # First verify — consumes.
        self.assertTrue(
            ct.verify_confirm_token(
                tok, "delete_invoice", {"id": "A"},
                "U-replay-reencode",
            ),
            "legit token must verify + consume",
        )
        # Second verify on the re-encoded variant — MUST fail
        # (nonce-keyed dedup catches the malleability).
        self.assertFalse(
            ct.verify_confirm_token(
                mutated_tok, "delete_invoice", {"id": "A"},
                "U-replay-reencode",
            ),
            "ak-dixt reviewer MAJOR: nonce-keyed dedup must catch "
            "the base64url-malleability replay — a re-encoded "
            "token string that decodes to the same payload bytes "
            "MUST NOT bypass one-time-use. Pre-fix (sha256(token)-"
            "keyed) this verified True; the fix moves the dedup "
            "key into the HMAC-protected payload (nonce) so all "
            "legit malleable variants collapse to the same key.",
        )

    # (15c) Pin the dedup-key SOURCE as `payload["nonce"]` via
    # source-grep. If a future refactor flips back to sha256(token)
    # or any other token-string-derived key, this test reds.
    def test_dedup_key_sources_from_payload_nonce(self):
        src = _read_source("utils/confirm_tokens.py")
        # The dedup key assignment must reference payload["nonce"].
        self.assertRegex(
            src,
            r"nonce\s*=\s*payload\.get\(\s*['\"]nonce['\"]\s*\)",
            "ak-dixt reviewer MAJOR: dedup key must source from "
            "payload['nonce'] (canonical, inside HMAC-protected "
            "payload). sha256(token)-keyed dedup is bypassable "
            "via base64url malleability — see test_replay_via_"
            "base64url_reencode_rejected",
        )
        # And the consumed-set add must use that nonce as the key.
        self.assertRegex(
            src,
            r"_consumed\[\s*nonce\s*\]\s*=",
            "ak-dixt reviewer MAJOR: consumed-set must be keyed "
            "on the nonce variable (sourced from payload), not on "
            "a hash of the token string",
        )
        # And the sha256(token) shape must be fully removed from
        # the dedup path. (Source-grep: no `sha256(token` anywhere
        # in utils/confirm_tokens.py.)
        self.assertNotRegex(
            src,
            r"hashlib\.sha256\(\s*token\.encode",
            "ak-dixt reviewer MAJOR: sha256(token.encode(...)) "
            "pattern must NOT survive — the b64url-malleable "
            "dedup key class is closed",
        )

    # (16) Constant-time compare: a short-sig doesn't raise.
    def test_short_signature_no_raise(self):
        from utils import confirm_tokens as ct
        tok = ct.issue_confirm_token("t", {}, "U")
        b64, _sig = tok.rsplit(".", 1)
        short_tok = f"{b64}.abc"
        # Must return False, not raise.
        self.assertFalse(
            ct.verify_confirm_token(short_tok, "t", {}, "U"),
        )

    # (17) Args-ordering independence (dict iteration order).
    def test_args_ordering_independent(self):
        from utils import confirm_tokens as ct
        # Issue with one order
        tok = ct.issue_confirm_token("t", {"a": 1, "b": 2}, "U")
        # Verify with the opposite order — must succeed because
        # args_hash uses sort_keys internally.
        self.assertTrue(
            ct.verify_confirm_token(
                tok, "t", {"b": 2, "a": 1}, "U",
            ),
            "ak-dixt: args_hash must be dict-order-independent "
            "(sort_keys), else legitimate FE round-trips fail",
        )

    # Missing-secret fail-closed on verify.
    def test_verify_fail_closed_without_secret(self):
        from utils import confirm_tokens as ct
        # Issue a token with secret set (via setUp), then strip the
        # secret and verify — must return False.
        tok = ct.issue_confirm_token("t", {}, "U")
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("AK_CONFIRM_SECRET", None)
            self.assertFalse(
                ct.verify_confirm_token(tok, "t", {}, "U"),
            )


# ─── Source-grep drift guards ────────────────────────────────


class TestAgentServiceGateShape(unittest.TestCase):
    """Pin the structural shape of the agentService.py gates +
    DESTRUCTIVE_TOOLS set. If someone removes the token path,
    reverts to pure client-asserted name-list, or drops
    update_invoice from the gate, these red immediately.
    """

    def setUp(self):
        self.src = _read_source("services/agentService.py")

    # (18) update_invoice added to DESTRUCTIVE_TOOLS.
    def test_update_invoice_in_destructive_tools(self):
        m = re.search(
            r"DESTRUCTIVE_TOOLS\s*=\s*\{([\s\S]*?)\}",
            self.src,
        )
        self.assertIsNotNone(
            m, "DESTRUCTIVE_TOOLS set not found — shape changed",
        )
        self.assertIn(
            '"update_invoice"', m.group(1),
            "ak-dixt: update_invoice must be a member of "
            "DESTRUCTIVE_TOOLS so its mutations require a confirm "
            "token. Pre-ak-dixt the tool was ungated.",
        )

    # (19) _is_call_confirmed helper present.
    def test_is_call_confirmed_helper_present(self):
        self.assertRegex(
            self.src,
            r"def\s+_is_call_confirmed\s*\(",
            "ak-dixt: _is_call_confirmed helper missing — it is "
            "the dual-accept gate (server-signed first, legacy "
            "name-list fallback with WARN)",
        )

    # (20) Both gates call _is_call_confirmed (not raw name-list).
    def test_destructive_gate_uses_is_call_confirmed(self):
        # Both gates (insert-after-web + destructive) must call the
        # helper. Count globally in agentService.py — the helper
        # is itself declared once, both call sites add two more.
        gate_calls = re.findall(
            r"_is_call_confirmed\(_tn,\s*args\)", self.src,
        )
        self.assertGreaterEqual(
            len(gate_calls), 2,
            f"ak-dixt: BOTH gates (insert-after-web + destructive) "
            f"must call _is_call_confirmed(_tn, args). Found "
            f"{len(gate_calls)} call site(s).",
        )
        # The raw client-asserted check `_tn not in confirmed_tools`
        # must NOT survive in agentService.py AS CODE — pre-ak-dixt
        # shape. Docstring / comment MENTIONS of the migrated phrase
        # are OK (and desirable for grep-ability), so strip comments
        # + docstrings before checking.
        code_only = []
        in_docstring = False
        for line in self.src.splitlines():
            stripped = line.strip()
            # Flip docstring state on triple-quote.
            tq = stripped.count('"""') + stripped.count("'''")
            if tq % 2:
                in_docstring = not in_docstring
                continue
            if in_docstring:
                continue
            if stripped.startswith("#"):
                continue
            code_only.append(line)
        code_src = "\n".join(code_only)
        self.assertNotIn(
            "_tn not in confirmed_tools", code_src,
            "ak-dixt: raw client-asserted check `_tn not in "
            "confirmed_tools` must NOT survive AS CODE in the "
            "handler — migrate to _is_call_confirmed. (Comment / "
            "docstring mentions are OK.)",
        )

    # (21) confirm events carry a confirm_id (minted via
    # issue_confirm_token).
    def test_confirm_events_mint_confirm_id(self):
        # Both confirm emits must call issue_confirm_token. Two
        # call sites expected (insert-after-web + destructive).
        mint_calls = re.findall(
            r"issue_confirm_token\(_tn,\s*args,\s*user_id\)",
            self.src,
        )
        self.assertGreaterEqual(
            len(mint_calls), 2,
            f"ak-dixt: both confirm event emits (insert-after-web "
            f"+ destructive) must call issue_confirm_token("
            f"_tn, args, user_id). Found {len(mint_calls)} "
            f"call site(s).",
        )
        self.assertIn(
            '"confirm_id"', self.src,
            "ak-dixt: confirm event payload must carry "
            "`confirm_id` key so FE can echo it on the retry turn",
        )

    # (22) Import present.
    def test_imports_confirm_tokens(self):
        self.assertRegex(
            self.src,
            r"from\s+utils\.confirm_tokens\s+import\s+"
            r"[\s\S]*?issue_confirm_token[\s\S]*?"
            r"verify_confirm_token",
            "ak-dixt: agentService must import issue_confirm_token "
            "+ verify_confirm_token from utils.confirm_tokens",
        )


class TestControllerDualAccept(unittest.TestCase):

    def setUp(self):
        self.src = _read_source("controllers/agentEP.py")

    # (23) Controller parses confirmed_calls alongside legacy.
    def test_controller_parses_both_fields(self):
        self.assertRegex(
            self.src,
            r"confirmed_tools\s*=\s*data\.get\(['\"]confirmed_tools",
            "ak-dixt: controller must keep parsing legacy "
            "`confirmed_tools` for the dual-accept migration window",
        )
        self.assertRegex(
            self.src,
            r"confirmed_calls\s*=\s*data\.get\(['\"]confirmed_calls",
            "ak-dixt: controller must parse the new "
            "`confirmed_calls` field (server-signed tokens)",
        )

    # (24) Controller threads confirmed_calls into stream_chat.
    def test_controller_threads_confirmed_calls(self):
        # The stream_chat kwargs span multiple lines; the inline
        # comment contains a stray `)` that breaks a non-greedy
        # paren match. Assert the kwarg appears SOMEWHERE between
        # the `stream_chat(` open and the first line containing
        # only `            )` (closing paren with 12-space indent,
        # the actual call close).
        m = re.search(
            r"self\.agent_service\.stream_chat\(([\s\S]*?)\n\s{12}\)",
            self.src,
        )
        self.assertIsNotNone(
            m, "stream_chat call site not found with expected "
            "closing-paren shape",
        )
        self.assertIn(
            "confirmed_calls=confirmed_calls", m.group(1),
            "ak-dixt: stream_chat call must pass "
            "confirmed_calls=confirmed_calls",
        )


class TestLegacyWarnLogShape(unittest.TestCase):
    """Pin that the legacy name-list path still works (dual-accept
    migration window) AND emits a WARN log tagged [AK-DIXT-LEGACY].
    """

    def setUp(self):
        self.src = _read_source("services/agentService.py")

    # (25) Legacy path logs [AK-DIXT-LEGACY] tag.
    def test_legacy_warn_log_tag_present(self):
        self.assertIn(
            "[AK-DIXT-LEGACY]", self.src,
            "ak-dixt: legacy `confirmed_tools` acceptance must "
            "WARN-log with [AK-DIXT-LEGACY] prefix for the 7-day "
            "zero-warn-log window before removal",
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
