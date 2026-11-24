"""ak-3oh: READ-ONLY audit of non-INR invoice payments (pre-ak-lvu
double-conversion review). Classification rules: utils/legacy_fx_audit.py.

Prints COUNTS and internal INVOICE IDS only (invoice numbers are editable
and may embed client initials): no amounts, rates, ratios, client names or
addresses. Never writes, and never boots the app (no import of the app module:
that runs db.create_all() DDL and can START the schedulers, which write to
prod). It builds a bare SQLAlchemy engine from DATABASE_URL, puts that
connection in a READ ONLY transaction (MySQL) before any query, and rolls
back at the end on every dialect. AK_SKIP_SCHEDULERS=1 is also set before
any import, in case something imports app transitively.

Usage (operator / infra):
    python scripts/ak3oh_legacy_fx_audit.py [--user-id <uid>]
Exit code: 0 = no suspect invoices, 3 = suspects listed (review), 1 = error.
"""
from __future__ import annotations

import argparse
import os
import sys

# Review MAJOR: before ANY project import (belt and braces; app is never
# imported here).
os.environ["AK_SKIP_SCHEDULERS"] = "1"

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from utils import legacy_fx_audit as A  # noqa: E402


def _enum_value(v):
    return v.value if hasattr(v, "value") else (str(v) if v is not None else None)


def build_report(session, user_id=None):
    """Read-only scan -> legacy_fx_audit.Report."""
    from models.freelance_management import Invoice, InvoicePayment
    q = session.query(Invoice.id, Invoice.invoice_number, Invoice.currency,
                      Invoice.status, Invoice.total)
    if user_id:
        q = q.filter(Invoice.user_id == user_id)
    invoices = [r for r in q.all() if (_enum_value(r.currency) or "").upper() != "INR"]
    report = A.Report()
    if not invoices:
        return report
    ids = [r.id for r in invoices]
    payments = {}
    for p in session.query(
            InvoicePayment.invoice_id, InvoicePayment.amount_received,
            InvoicePayment.original_amount, InvoicePayment.original_currency,
            InvoicePayment.inr_amount, InvoicePayment.fx_rate,
    ).filter(InvoicePayment.invoice_id.in_(ids)).all():
        payments.setdefault(p.invoice_id, []).append({
            "amount_received": p.amount_received, "original_amount": p.original_amount,
            "original_currency": p.original_currency, "inr_amount": p.inr_amount,
            "fx_rate": p.fx_rate,
        })
    for r in invoices:
        label = A.classify_invoice(_enum_value(r.currency), _enum_value(r.status), r.total,
                                   payments.get(r.id, []))
        report.add(r.id, _enum_value(r.currency), label)
    return report


def open_read_only_session(url):
    """A bare session on ONE connection (no app init, no create_all). On
    MySQL the connection is set READ ONLY before any query."""
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import Session
    engine = create_engine(url, pool_pre_ping=True)
    conn = engine.connect()
    if engine.dialect.name == "mysql":
        conn.execute(text("SET SESSION TRANSACTION READ ONLY"))
    return engine, conn, Session(bind=conn), engine.dialect.name


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--user-id", help="restrict to one user (default: all invoices)")
    args = p.parse_args(argv)
    try:
        from dotenv import load_dotenv     # same .env the app reads
        load_dotenv()
    except ImportError:
        pass
    url = os.getenv("DATABASE_URL")
    if not url:
        print("[3oh] DATABASE_URL is not set", file=sys.stderr)
        return 1
    engine = conn = session = None
    try:
        engine, conn, session, dialect = open_read_only_session(url)
        report = build_report(session, args.user_id)
    except Exception as exc:
        print(f"[3oh] audit failed ({type(exc).__name__})", file=sys.stderr)
        return 1
    finally:
        if session is not None:
            session.rollback()
            session.close()
        if conn is not None:
            conn.close()
        if engine is not None:
            engine.dispose()
    print(f"[3oh] READ-ONLY audit (dialect={dialect or '?'}, "
          f"user={'all' if not args.user_id else 'one'})")
    for line in report.lines():
        print(f"[3oh] {line}")
    suspects = sum(len(report.by_class.get(c, [])) for c in A.SUSPECT_CLASSES)
    print(f"[3oh] suspect invoices: {suspects}")
    return 3 if suspects else 0


if __name__ == "__main__":
    sys.exit(main())
