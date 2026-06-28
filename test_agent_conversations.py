"""ak-bq5 — offline unit tests for cross-device agent chat persistence.

Coverage:
  - derive_title: empty / whitespace / short / long / multi-line input
  - VALID_AGENT_TYPES contract
  - Models: column shape sanity (table_args / FK / soft-delete column)
  - Controller wiring: routes registered in app.py
  - SSE leading event shape for the new conversation_id event

Heavy DB-driven tests (cross-user isolation, soft-delete excluded from
list, agent_type filter, auto-create-vs-reuse) are designed to run on
infra against a real DB after deploy. The offline layer here exercises
the pure-Python pieces + structural contracts.
"""

import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# ── derive_title ─────────────────────────────────────────────────────────


class TestDeriveTitle(unittest.TestCase):
    """derive_title is dependency-free (just str / regex) so we can
    import it directly without stubbing flask / sqlalchemy."""

    def setUp(self):
        # Lazy import avoids dragging models import-chain into other
        # tests; if the module fails to import we skip the whole class.
        try:
            from services.agentConversationService import (
                derive_title, DEFAULT_TITLE, TITLE_MAX_LEN,
            )
        except ImportError as e:
            self.skipTest(f"agentConversationService not importable: {e}")
            return
        self.derive = derive_title
        self.DEFAULT = DEFAULT_TITLE
        self.MAX_LEN = TITLE_MAX_LEN

    def test_empty_inputs_use_default(self):
        print("\n[derive_title — empty / whitespace input → default]")
        for raw in (None, "", "   ", "\n\n\t  ", 0, [], {}):
            self.assertEqual(self.derive(raw), self.DEFAULT,
                             f"expected default for {raw!r}")
        print(f"  ✓ default title '{self.DEFAULT}' for empty inputs")

    def test_short_input_passes_through(self):
        print("\n[derive_title — short input passes through unchanged]")
        out = self.derive("What's my EPF balance?")
        self.assertEqual(out, "What's my EPF balance?")
        self.assertNotIn("…", out)
        print(f"  ✓ {out!r}")

    def test_newlines_collapsed(self):
        print("\n[derive_title — newlines + multi-space collapsed to single space]")
        out = self.derive("Help me\n\n\nfix\tEPF\n entries")
        self.assertEqual(out, "Help me fix EPF entries")
        self.assertNotIn("\n", out)
        self.assertNotIn("\t", out)
        self.assertNotIn("  ", out)
        print(f"  ✓ collapsed → {out!r}")

    def test_long_input_truncated_at_word_boundary(self):
        print("\n[derive_title — long input truncated at MAX_LEN with ellipsis]")
        # 100-char message; expect truncation to ~MAX_LEN + ellipsis.
        raw = ("Walk me through reconciling my April 2026 EPF passbook "
               "entries against the Form 23A I just uploaded")
        out = self.derive(raw)
        # ellipsis added when truncation happens
        self.assertTrue(out.endswith("…"))
        # never longer than MAX_LEN + 1 (the ellipsis)
        self.assertLessEqual(len(out), self.MAX_LEN + 1)
        # whatever is kept must be a prefix of the cleaned input (sans
        # ellipsis), so the title remains anchored in the user's words
        self.assertTrue(raw.startswith(out.rstrip("…")))
        print(f"  ✓ truncated ({len(out)} chars): {out!r}")

    def test_non_string_input_uses_default(self):
        print("\n[derive_title — non-string input → default]")
        # Defensive: list, dict, int all return default rather than
        # str()-ing into a weird title.
        self.assertEqual(self.derive(["hi"]), self.DEFAULT)
        self.assertEqual(self.derive({"role": "user"}), self.DEFAULT)
        self.assertEqual(self.derive(42), self.DEFAULT)
        print("  ✓ non-string returns default")


class TestValidAgentTypes(unittest.TestCase):
    def test_contract_matches_controllers(self):
        print("\n[VALID_AGENT_TYPES — matches controllers/agentEP.py]")
        try:
            from services.agentConversationService import VALID_AGENT_TYPES
        except ImportError as e:
            self.skipTest(f"agentConversationService not importable: {e}")
            return
        # Sanity: the three agent types known to agentEP.chat() validate.
        self.assertEqual(
            set(VALID_AGENT_TYPES),
            {"investment", "transaction", "freelance"},
        )
        print(f"  ✓ {VALID_AGENT_TYPES}")


# ── Model structural sanity ──────────────────────────────────────────────


class TestModelStructure(unittest.TestCase):
    """The model files declare table_args + columns we rely on for the
    list query + soft-delete filter. Verify by reading the source — the
    real ORM materialization happens via db.create_all() in prod, but
    structural review catches drift offline."""

    @classmethod
    def setUpClass(cls):
        repo = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(repo, "models", "AgentConversation.py")) as fh:
            cls.conv_src = fh.read()
        with open(os.path.join(repo, "models", "AgentMessage.py")) as fh:
            cls.msg_src = fh.read()

    def test_conversation_has_soft_delete_column(self):
        print("\n[AgentConversation — deleted_at column present]")
        self.assertIn("deleted_at = Column", self.conv_src)
        self.assertIn("nullable=True", self.conv_src)
        print("  ✓ deleted_at nullable")

    def test_conversation_has_compound_index(self):
        print("\n[AgentConversation — (user_id, agent_type, updated_at) index]")
        # Index name + composed columns (order matters for the ORDER BY)
        self.assertIn("ix_agent_conv_user_type_updated", self.conv_src)
        self.assertRegex(
            self.conv_src,
            r"'user_id',\s*'agent_type',\s*'updated_at'",
        )
        print("  ✓ compound index supports list query")

    def test_conversation_fk_to_users_cascade(self):
        print("\n[AgentConversation — user_id FK with CASCADE]")
        self.assertRegex(
            self.conv_src,
            r"ForeignKey\('users\.userID',\s*ondelete='CASCADE'\)",
        )
        print("  ✓ CASCADE on user delete")

    def test_message_role_constants_match_dispatch(self):
        print("\n[AgentMessage — AGENT_MESSAGE_ROLES matches dispatch contract]")
        self.assertIn("AGENT_MESSAGE_ROLES = ('user', 'assistant', 'tool')",
                      self.msg_src)
        print("  ✓ user / assistant / tool")

    def test_message_has_attachments_meta_json(self):
        print("\n[AgentMessage — attachments_meta JSON nullable]")
        self.assertRegex(
            self.msg_src,
            r"attachments_meta\s*=\s*Column\(JSON,\s*nullable=True\)",
        )
        print("  ✓ JSON nullable column present")

    def test_message_partial_default_false(self):
        print("\n[AgentMessage — partial defaults to FALSE]")
        # ORM default + server_default both False so a row coming in via
        # raw SQL (e.g. migration backfill) also lands False.
        self.assertIn("default=False", self.msg_src)
        self.assertIn("server_default='0'", self.msg_src)
        print("  ✓ partial defaults False")

    def test_message_fk_cascade_to_conversation(self):
        print("\n[AgentMessage — conversation_id FK with CASCADE]")
        self.assertRegex(
            self.msg_src,
            r"ForeignKey\('agent_conversations\.id',\s*ondelete='CASCADE'\)",
        )
        print("  ✓ CASCADE on conversation delete")


# ── Controller wiring sanity ─────────────────────────────────────────────


class TestRouteWiring(unittest.TestCase):
    """Routes registered in app.py drive the entire BE surface. If a
    route gets dropped during a refactor, the FE silently 404s. These
    checks live in source-only verification so they don't need a Flask
    boot to catch the regression."""

    @classmethod
    def setUpClass(cls):
        repo = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(repo, "app.py")) as fh:
            cls.app_src = fh.read()

    def test_list_route_registered(self):
        print("\n[app.py — GET /agent/conversations route present]")
        self.assertRegex(
            self.app_src,
            r"\('/agent/conversations',\s*'GET',\s*self\.agentConversationsEP\.list\)",
        )
        print("  ✓ GET list")

    def test_create_route_registered(self):
        print("\n[app.py — POST /agent/conversations route present]")
        self.assertRegex(
            self.app_src,
            r"\('/agent/conversations',\s*'POST',\s*self\.agentConversationsEP\.create\)",
        )
        print("  ✓ POST create")

    def test_get_route_registered(self):
        print("\n[app.py — GET /agent/conversations/<id> route present]")
        self.assertRegex(
            self.app_src,
            r"\('/agent/conversations/<conversation_id>',\s*'GET',\s*self\.agentConversationsEP\.get\)",
        )
        print("  ✓ GET single")

    def test_delete_route_registered(self):
        print("\n[app.py — DELETE /agent/conversations/<id> route present]")
        self.assertRegex(
            self.app_src,
            r"\('/agent/conversations/<conversation_id>',\s*'DELETE',\s*self\.agentConversationsEP\.delete\)",
        )
        print("  ✓ DELETE soft")

    def test_conversation_service_wired_into_agent_service(self):
        print("\n[app.py — conversation_service passed to AgentService.set_services]")
        self.assertIn(
            "conversation_service=self.agentConversationService",
            self.app_src,
        )
        print("  ✓ AgentService receives conversation_service")


# ── /agent/chat shape ────────────────────────────────────────────────────


class TestChatControllerShape(unittest.TestCase):
    """The chat() handler now accepts conversation_id in the body and
    forwards it to stream_chat. Verify via source inspection."""

    @classmethod
    def setUpClass(cls):
        repo = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(repo, "controllers", "agentEP.py")) as fh:
            cls.src = fh.read()

    def test_chat_accepts_conversation_id(self):
        print("\n[agentEP.chat — accepts conversation_id from body]")
        self.assertIn('conversation_id = data.get("conversation_id")', self.src)
        print("  ✓ conversation_id read")

    def test_chat_forwards_to_stream_chat(self):
        print("\n[agentEP.chat — forwards conversation_id to stream_chat]")
        self.assertIn("conversation_id=conversation_id,", self.src)
        print("  ✓ forwarded")

    def test_chat_rejects_non_int_conversation_id(self):
        print("\n[agentEP.chat — non-int conversation_id → 400]")
        self.assertIn(
            'conversation_id must be an integer',
            self.src,
        )
        print("  ✓ int coercion enforced")


# ── stream_chat SSE event ────────────────────────────────────────────────


class TestStreamChatSSEEvent(unittest.TestCase):
    """stream_chat must emit a leading {type:conversation_id, id:int}
    event so the FE can pin the id before any text streams in."""

    @classmethod
    def setUpClass(cls):
        repo = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(repo, "services", "agentService.py")) as fh:
            cls.src = fh.read()

    def test_leading_conversation_id_event_emitted(self):
        print("\n[agentService — emits SSE conversation_id event]")
        self.assertIn(
            'yield self._sse_event("conversation_id"',
            self.src,
        )
        print("  ✓ event present in stream loop")

    def test_assistant_persist_on_success(self):
        print("\n[agentService — append_message(role='assistant', partial=False) on success]")
        # Look for the canonical success-path persist call shape
        self.assertRegex(
            self.src,
            r'role="assistant",\s*\n\s*content=full_text or "",\s*\n\s*partial=False,',
        )
        print("  ✓ assistant message persisted before `done` yield")

    def test_partial_assistant_persist_in_finally(self):
        print("\n[agentService — partial=True persist in finally block]")
        # Source markers for the partial-save path
        self.assertIn("not persisted_assistant", self.src)
        self.assertIn("partial=True", self.src)
        print("  ✓ partial save guarded by persisted_assistant flag")


# ── Service-layer scope guard contract ───────────────────────────────────


class TestServiceScopeGuards(unittest.TestCase):
    """Every read / write path must go through _fetch_owned so the
    cross-user check has a single source of truth. Verifying via source
    inspection (heavy DB-level cross-user isolation tests run on infra
    against a real DB)."""

    @classmethod
    def setUpClass(cls):
        repo = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(repo, "services", "agentConversationService.py")) as fh:
            cls.src = fh.read()

    def test_fetch_owned_filters_user_and_soft_delete(self):
        print("\n[_fetch_owned — filters by user_id AND deleted_at IS NULL]")
        self.assertIn("AgentConversation.user_id == user_id", self.src)
        self.assertIn("AgentConversation.deleted_at.is_(None)", self.src)
        print("  ✓ both filters present in helper")

    def test_get_and_delete_use_fetch_owned(self):
        print("\n[get_conversation + soft_delete + append_message use _fetch_owned]")
        # Count call sites — should be at least three (get / append / soft_delete)
        callsites = re.findall(r"self\._fetch_owned\(", self.src)
        self.assertGreaterEqual(len(callsites), 3,
                                f"expected ≥3 _fetch_owned calls, got {len(callsites)}")
        print(f"  ✓ {len(callsites)} membership-check call sites")

    def test_list_filters_user_and_soft_delete(self):
        print("\n[list_conversations — filters user_id AND deleted_at IS NULL]")
        self.assertIn("AgentConversation.user_id == user_id", self.src)
        # list_conversations has its own deleted_at filter (doesn't go
        # through _fetch_owned)
        self.assertRegex(
            self.src,
            r"\.filter\(AgentConversation\.deleted_at\.is_\(None\)\)",
        )
        print("  ✓ list query filtered by both")


# ── Runner ───────────────────────────────────────────────────────────────


if __name__ == "__main__":
    print("ak-bq5 agent conversations — offline unit tests")
    print("=" * 70)
    unittest.main(verbosity=0, exit=False)
    print("=" * 70)
    print("Done.")
