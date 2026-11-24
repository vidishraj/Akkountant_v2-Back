"""ak-ep4i (2026-11-24): transaction drawer annotations — category
(Keep / Change…), merchant rules and notes.

Covers services/transactionAnnotations.py on in-memory SQLite, the
merchant-rule hook in TransactionService.insertTransactions (AST-lifted),
and route / controller wiring by source inspection.

Lead requirement: applying a merchant rule at import must change ONLY
``tag``; never amount, date or referenceID (TestInsertRuleOnlyTouchesTag).
All rows are synthetic.
"""
from __future__ import annotations

import ast
import os
import re
import sys
import unittest
from datetime import datetime
from decimal import Decimal

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from services import transactionAnnotations as ta  # noqa: E402
from services.transactionAnnotations import AnnotationError, rule_tag_for  # noqa: E402

try:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import scoped_session, sessionmaker
    import models  # noqa: F401
    from models.investmentHistory import InvestmentHistory  # noqa: F401 (mapper config)
    from models.transactions import Transactions, ProcessingMethod
    from models.savedTags import SavedTags
    from models.transactionNote import TransactionNote
    _DEPS_OK, _SKIP = True, ""
except Exception as _exc:  # pragma: no cover
    _DEPS_OK, _SKIP = False, f"model import failed: {_exc}"

ME, OTHER = "me_uid", "other_uid"


def _engine():
    engine = create_engine("sqlite:///:memory:")
    for t in (Transactions.__table__, SavedTags.__table__, TransactionNote.__table__):
        t.create(engine)
    return engine


class _DB(unittest.TestCase):

    def setUp(self):
        self.factory = scoped_session(sessionmaker(bind=_engine()))
        self.s = self.factory()

    def tearDown(self):
        self.factory.remove()

    def add_txn(self, ref, details, user=ME, tag="", amount="100.00"):
        self.s.add(Transactions(
            referenceID=ref, user=user, date=datetime(2026, 11, 1),
            details=details, tag=tag, amount=Decimal(amount), bank="HDFC",
            source="Statement", processed_via=ProcessingMethod.CLAUDE_CODE,
        ))
        self.s.commit()

    def tag_of(self, ref):
        self.s.expire_all()
        return self.s.query(Transactions).filter_by(referenceID=ref).one().tag

    def err(self, fn, *args):
        with self.assertRaises(AnnotationError) as cm:
            fn(self.s, *args)
        return cm.exception


# ── pure rule matching ──────────────────────────────────────────────

class TestRuleTagFor(unittest.TestCase):

    def test_substring_case_insensitive(self):
        self.assertEqual(rule_tag_for([("grab.com", "Transport")], "ECOM NP WWW.GRAB.COM"), "Transport")

    def test_longest_pattern_wins(self):
        rules = [("GRAB", "Transport"), ("GRAB.COM FOOD", "Food"), ("ECOM", "Shopping")]
        self.assertEqual(rule_tag_for(rules, "ECOM WWW.GRAB.COM FOOD 123"), "Food")
        self.assertEqual(rule_tag_for(list(reversed(rules)), "ECOM WWW.GRAB.COM FOOD 123"), "Food")

    def test_no_match_or_empty(self):
        self.assertIsNone(rule_tag_for([("AMAZON", "Shopping")], "ECOM GRAB"))
        self.assertIsNone(rule_tag_for([], "x"))
        self.assertIsNone(rule_tag_for([("GRAB", "")], "GRAB"))
        self.assertIsNone(rule_tag_for([("GRAB", "T")], None))


# ── category ────────────────────────────────────────────────────────

@unittest.skipUnless(_DEPS_OK, _SKIP)
class TestSetCategory(_DB):

    def setUp(self):
        super().setUp()
        self.add_txn("r1", "ECOM NP WWW.GRAB.COM 4711")
        self.add_txn("r2", "UPI GRAB.COM RIDE", tag="")
        self.add_txn("r3", "POS GRAB.COM", tag="Already set")
        self.add_txn("r4", "ATM CASH")
        self.add_txn("theirs", "ECOM NP WWW.GRAB.COM", user=OTHER)

    def test_keep_and_change_persist_tag(self):
        res = ta.set_category(self.s, ME, "r1", "  Transport  ")
        self.assertEqual(res, {"reference_id": "r1", "category": "Transport", "rule": None})
        self.assertEqual(self.tag_of("r1"), "Transport")
        ta.set_category(self.s, ME, "r1", "Food & dining")
        self.assertEqual(self.tag_of("r1"), "Food & dining")

    def test_clear(self):
        ta.set_category(self.s, ME, "r1", "Transport")
        for clear in (None, "", "   "):
            res = ta.set_category(self.s, ME, "r1", clear)
            self.assertIsNone(res["category"])
            self.assertEqual(self.tag_of("r1"), "")

    def test_other_users_and_unknown_are_404(self):
        e = self.err(ta.set_category, ME, "theirs", "Hacked")
        self.assertEqual((e.code, e.status), ("not_found", 404))
        self.assertEqual(self.tag_of("theirs"), "")
        self.assertEqual(self.err(ta.set_category, ME, "nope", "X").status, 404)

    def test_invalid_category(self):
        for bad in (5, ["x"], "x" * 101):
            e = self.err(ta.set_category, ME, "r1", bad)
            self.assertEqual((e.code, e.status), ("invalid_category", 400))
        self.assertEqual(self.tag_of("r1"), "")
        ta.set_category(self.s, ME, "r1", "x" * 100)
        self.assertEqual(self.tag_of("r1"), "x" * 100)

    def test_invalid_reference(self):
        for bad in ("a/b", "", "x" * 65, "r1\n", "r 1", None, 7):
            e = self.err(ta.set_category, ME, bad, "X")
            self.assertEqual(e.code, "invalid_reference")

    def test_missing_uid_refused(self):
        self.assertEqual(self.err(ta.set_category, None, "r1", "X").status, 401)

    def test_rule_created_without_backfill(self):
        res = ta.set_category(self.s, ME, "r1", "Transport", {"pattern": " grab.com "})
        self.assertEqual(res["rule"]["pattern"], "grab.com")
        self.assertEqual(res["rule"]["category"], "Transport")
        self.assertEqual(res["rule"]["applied_to_existing"], 0)
        self.assertIsInstance(res["rule"]["id"], int)
        self.assertEqual(self.tag_of("r2"), "")  # not backfilled
        rows = self.s.query(SavedTags).all()
        self.assertEqual([(r.user, r.details, r.tag) for r in rows], [(ME, "grab.com", "Transport")])

    def test_rule_backfill_only_own_untagged_matches(self):
        res = ta.set_category(self.s, ME, "r1", "Transport",
                              {"pattern": "GRAB.COM", "apply_to_existing": True})
        # r1 itself is now tagged; r2 is the only other untagged match.
        self.assertEqual(res["rule"]["applied_to_existing"], 1)
        self.assertEqual(self.tag_of("r2"), "Transport")
        self.assertEqual(self.tag_of("r3"), "Already set")  # never overwritten
        self.assertEqual(self.tag_of("r4"), "")              # no match
        self.assertEqual(self.tag_of("theirs"), "")          # other user

    def test_backfill_like_wildcards_are_literal(self):
        self.add_txn("w1", "PAY 50%_OFF STORE")
        self.add_txn("w2", "PAY 50XYOFF STORE")
        ta.set_category(self.s, ME, "w1", "Deals", {"pattern": "50%_OFF", "apply_to_existing": True})
        self.assertEqual(self.tag_of("w2"), "")

    def test_rule_upsert_case_insensitive(self):
        a = ta.set_category(self.s, ME, "r1", "Transport", {"pattern": "grab.com"})
        b = ta.set_category(self.s, ME, "r1", "Food", {"pattern": "GRAB.COM"})
        self.assertEqual(a["rule"]["id"], b["rule"]["id"])
        rows = self.s.query(SavedTags).all()
        self.assertEqual([(r.details, r.tag) for r in rows], [("GRAB.COM", "Food")])

    def test_invalid_rules(self):
        cases = [
            ("Transport", "not-a-dict"),
            ("Transport", {"pattern": 5}),
            ("Transport", {"pattern": "ab"}),
            ("Transport", {"pattern": "x" * 101}),
            ("Transport", {"pattern": "AMAZON"}),              # not in memo
            ("Transport", {"pattern": "XX••••4711"}),          # masked display text
            ("Transport", {"pattern": "GRAB", "apply_to_existing": "yes"}),
            (None, {"pattern": "GRAB"}),                       # rule with a clear
        ]
        for category, rule in cases:
            e = self.err(ta.set_category, ME, "r1", category, rule)
            self.assertEqual((e.code, e.status), ("invalid_rule", 400), (category, rule))
        self.assertEqual(self.tag_of("r1"), "")  # nothing written on a 400
        self.assertEqual(self.s.query(SavedTags).count(), 0)


# ── note ────────────────────────────────────────────────────────────

@unittest.skipUnless(_DEPS_OK, _SKIP)
class TestSetNote(_DB):

    def setUp(self):
        super().setUp()
        self.add_txn("r1", "MEMO")
        self.add_txn("theirs", "MEMO", user=OTHER)

    def notes(self):
        self.s.expire_all()
        return [(n.user, n.referenceID, n.note) for n in self.s.query(TransactionNote).all()]

    def test_set_update_clear(self):
        self.assertEqual(ta.set_note(self.s, ME, "r1", "  dinner w/ A  "),
                         {"reference_id": "r1", "note": "dinner w/ A"})
        ta.set_note(self.s, ME, "r1", "edited")
        self.assertEqual(self.notes(), [(ME, "r1", "edited")])
        for clear in (None, "", "  \n "):
            self.assertIsNone(ta.set_note(self.s, ME, "r1", clear)["note"])
            self.assertEqual(self.notes(), [])

    def test_length_limit_strict(self):
        ta.set_note(self.s, ME, "r1", "x" * 2000)
        e = self.err(ta.set_note, ME, "r1", "y" * 2001)
        self.assertEqual((e.code, e.status), ("note_too_long", 400))
        self.assertEqual(self.notes(), [(ME, "r1", "x" * 2000)])  # unchanged, not truncated

    def test_invalid_and_scoping(self):
        self.assertEqual(self.err(ta.set_note, ME, "r1", 5).code, "invalid_note")
        self.assertEqual(self.err(ta.set_note, ME, "theirs", "hi").status, 404)
        self.assertEqual(self.err(ta.set_note, ME, "a/b", "hi").code, "invalid_reference")
        self.assertEqual(self.notes(), [])

    def test_notes_for_scoped(self):
        ta.set_note(self.s, ME, "r1", "mine")
        ta.set_note(self.s, OTHER, "theirs", "not mine")
        self.assertEqual(ta.notes_for(self.s, ME, ["r1", "theirs", None]), {"r1": "mine"})
        self.assertEqual(ta.notes_for(self.s, ME, []), {})
        self.assertEqual(ta.notes_for(self.s, None, ["r1"]), {})

    def test_amount_date_untouched_by_annotations(self):
        ta.set_note(self.s, ME, "r1", "n")
        ta.set_category(self.s, ME, "r1", "C")
        self.s.expire_all()
        t = self.s.query(Transactions).filter_by(referenceID="r1").one()
        self.assertEqual((t.amount, t.date, t.details), (Decimal("100.00"), datetime(2026, 11, 1), "MEMO"))


@unittest.skipUnless(_DEPS_OK, _SKIP)
class TestListCategories(_DB):

    def test_distinct_sorted_scoped(self):
        self.add_txn("a", "M1", tag="food")
        self.add_txn("b", "M2", tag="Bills")
        self.add_txn("c", "M3", tag="Bills")
        self.add_txn("d", "M4", tag="")
        self.add_txn("e", "M5", tag="Secret", user=OTHER)
        self.s.add(SavedTags(user=ME, details="UBER", tag="Transport"))
        self.s.add(SavedTags(user=OTHER, details="X", tag="OtherRule"))
        self.s.commit()
        self.assertEqual(ta.list_categories(self.s, ME),
                         {"categories": ["Bills", "food", "Transport"]})


# ── insertTransactions: rule application only touches tag ───────────

def _lift_insert():
    with open(os.path.join(HERE, "services", "transactionsService.py")) as fh:
        tree = ast.parse(fh.read())
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "TransactionService")
    fns = [n for n in cls.body if isinstance(n, ast.FunctionDef)
           and n.name in ("insertTransactions", "_apply_tag_rules")]
    assert len(fns) == 2
    for f in fns:
        f.decorator_list = []
    mod = ast.Module(body=fns, type_ignores=[])
    ast.fix_missing_locations(mod)
    from sqlalchemy.exc import IntegrityError
    from enums.TransactionTypeEnum import TransactionTypeEnum
    from models.transactionsForReview import TransactionForReview
    ns = {
        "Transactions": Transactions, "ProcessingMethod": ProcessingMethod,
        "TransactionTypeEnum": TransactionTypeEnum, "IntegrityError": IntegrityError,
        "TransactionForReview": TransactionForReview, "TransactionInsertError": RuntimeError,
        "load_tag_rules": ta.load_tag_rules, "rule_tag_for": ta.rule_tag_for,
    }
    exec(compile(mod, "<lift:insertTransactions>", "exec"), ns)
    return ns


class _Log:
    def __getattr__(self, _name):
        return lambda *a, **k: None


class _DateUtil:
    def convert_to_sql_datetime(self, date_str, bank):
        return datetime.strptime(date_str, "%d/%m/%Y")


@unittest.skipUnless(_DEPS_OK, _SKIP)
class TestInsertRuleOnlyTouchesTag(unittest.TestCase):

    INPUT = [
        {"reference": "ref-grab", "date": "03/11/2026", "description": "ECOM NP WWW.GRAB.COM 99",
         "amount": Decimal("250.50"), "bank_reference_id": "B1"},
        {"reference": "ref-grabfood", "date": "04/11/2026", "description": "GRAB.COM FOOD 12",
         "amount": Decimal("-75.00")},
        {"reference": "ref-atm", "date": "05/11/2026", "description": "ATM CASH WDL",
         "amount": Decimal("2000.00")},
    ]

    def _run(self, rules, rule_user=ME):
        ns = _lift_insert()
        factory = scoped_session(sessionmaker(bind=_engine()))
        session = factory()
        for pattern, tag in rules:
            session.add(SavedTags(user=rule_user, details=pattern, tag=tag))
        session.commit()

        class Svc:
            db = type("DB", (), {"session": session})()
            logger = _Log()
            dateTimeUtil = _DateUtil()
            _apply_tag_rules = ns["_apply_tag_rules"]

        errors = ns["insertTransactions"](Svc(), [dict(t) for t in self.INPUT], "HDFC", ME, [],
                                          "Statement", fileId=None)
        session.expire_all()
        rows = {r.referenceID: r for r in session.query(Transactions).all()}
        snap = {ref: {c.name: getattr(r, c.name) for c in Transactions.__table__.columns}
                for ref, r in rows.items()}
        factory.remove()
        return errors, snap

    def test_rules_change_only_tag(self):
        e0, plain = self._run([])
        e1, ruled = self._run([("grab.com", "Transport"), ("GRAB.COM FOOD", "Food")])
        self.assertEqual((e0, e1), (0, 0))
        self.assertEqual(set(plain), {"ref-grab", "ref-grabfood", "ref-atm"})
        self.assertEqual(set(plain), set(ruled))  # same referenceIDs
        for ref in plain:
            a = {k: v for k, v in plain[ref].items() if k != "tag"}
            b = {k: v for k, v in ruled[ref].items() if k != "tag"}
            self.assertEqual(a, b, ref)  # amount, date, referenceID, details... identical
        self.assertEqual({r: plain[r]["tag"] for r in plain},
                         {"ref-grab": "", "ref-grabfood": "", "ref-atm": ""})
        self.assertEqual({r: ruled[r]["tag"] for r in ruled},
                         {"ref-grab": "Transport", "ref-grabfood": "Food", "ref-atm": ""})
        self.assertEqual(ruled["ref-grab"]["amount"], Decimal("250.50"))
        self.assertEqual(ruled["ref-grabfood"]["amount"], Decimal("-75.00"))
        self.assertEqual(ruled["ref-grab"]["date"], datetime(2026, 11, 3))

    def test_other_users_rules_ignored(self):
        _, ruled = self._run([("grab.com", "Transport")], rule_user=OTHER)
        self.assertTrue(all(r["tag"] == "" for r in ruled.values()))

    def test_rule_lookup_failure_never_blocks_import(self):
        ns = _lift_insert()
        factory = scoped_session(sessionmaker(bind=_engine()))
        session = factory()

        def boom(*_a):
            raise RuntimeError("db down")
        ns_apply = ns["_apply_tag_rules"]
        ns_apply.__globals__["load_tag_rules"] = boom

        class Svc:
            db = type("DB", (), {"session": session})()
            logger = _Log()
            dateTimeUtil = _DateUtil()
            _apply_tag_rules = ns_apply

        self.assertEqual(ns["insertTransactions"](Svc(), [dict(t) for t in self.INPUT], "HDFC",
                                                  ME, [], "Statement"), 0)
        self.assertEqual(session.query(Transactions).count(), 3)
        factory.remove()


# ── wiring (source inspection) ──────────────────────────────────────

class TestWiring(unittest.TestCase):

    def setUp(self):
        def read(p):
            with open(os.path.join(HERE, p)) as fh:
                return fh.read()
        self.app = read("app.py")
        self.ctrl = read("controllers/transactionsEP.py")
        self.svc = read("services/transactionsService.py")
        self.models = read("models/__init__.py")

    def test_routes(self):
        for rule, method, view in (
            ("/transactions/categories", "GET", "listTransactionCategories"),
            ("/transactions/<reference_id>/category", "PUT", "setTransactionCategory"),
            ("/transactions/<reference_id>/note", "PUT", "setTransactionNote"),
        ):
            pat = rf"\('{re.escape(rule)}',\s*'{method}',\s*self\.transactionEP\.{view}\)"
            self.assertRegex(self.app, pat)

    def test_controllers_use_session_uid(self):
        for name in ("setTransactionCategory", "setTransactionNote", "listTransactionCategories"):
            i = self.ctrl.index(f"def {name}(")
            body = self.ctrl[i:i + 900]
            self.assertIn('g.get("firebase_id")', body, name)

    def test_fetch_transactions_adds_note(self):
        i = self.ctrl.index("def fetchTransactions(")
        body = self.ctrl[i:self.ctrl.index("def fetchOptedBanks", i)]
        self.assertIn("fetchTransactionNotes(", body)
        self.assertIn('r["note"] = notes.get(', body)

    def test_insert_applies_rules_before_write(self):
        i = self.svc.index("def insertTransactions(")
        body = self.svc[i:i + 6000]
        self.assertLess(body.index("self._apply_tag_rules(transaction_objects, userId)"),
                        body.index("add_all(transaction_objects)"))

    def test_note_model_registered(self):
        self.assertIn("from models.transactionNote import TransactionNote", self.models)


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
