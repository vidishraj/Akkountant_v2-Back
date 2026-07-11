#!/usr/bin/env python3
"""ak-nyd: CLI to manage per-file PDF password overrides.

Usage:

  # Add or update a password for a specific fileID or gmail_id:
  python manage_pdf_password.py add \
      --gmail-id 19c248db3a7b7d55 --password 'S3cret!'
  python manage_pdf_password.py add \
      --file-id mail_pipeline_uxxx_HDFC_DEBIT_2026_01 --password 'S3cret!'

  # Look up an entry:
  python manage_pdf_password.py get --gmail-id 19c248db3a7b7d55

  # List all entries (values ELIDED — shows keys only):
  python manage_pdf_password.py list

  # Remove:
  python manage_pdf_password.py remove --gmail-id 19c248db3a7b7d55

  # Use an explicit path (defaults to $AK_PER_FILE_PASSWORDS_PATH or
  # ~/.config/akkountant/per_file_passwords.json — ak-nyd v2):
  python manage_pdf_password.py --path /etc/akkountant/pw.json list

MailProcessor's reprocess_pdf reads this store before falling back to
_try_personal_info_passwords. See utils/per_file_passwords.py for
lookup semantics + security notes.

ak-nyd v2 security hardening (per reviewer BOUNCE):
  - Default path is now ~/.config/akkountant/per_file_passwords.json
    (OUTSIDE the source tree — v1 defaulted to CWD which risked
    accidental git commits).
  - The store file is written 0o600 (owner rw only) after every
    write. Parent dir is 0o700 when we create it.
  - Backend .gitignore blocks per_file_passwords.json at any depth
    so an operator running an old (pre-v2) CLI can't accidentally
    commit the file.
  - Passwords still plaintext on disk — proper encrypted store lands
    under ak-2ln umbrella.

dispatched_by: akkountant/crew/akkountant_lead (hq-wisp-6n526p)
bead: ak-nyd
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.per_file_passwords import (
    add_password,
    list_entries,
    lookup_password,
    remove_password,
)


def _cmd_add(args):
    if not args.gmail_id and not args.file_id:
        raise SystemExit("add: --gmail-id or --file-id required")
    if not args.password:
        raise SystemExit("add: --password required")
    add_password(
        args.password,
        gmail_id=args.gmail_id, file_id=args.file_id,
        path=args.path,
    )
    print(
        f"[ADDED] gmail_id={args.gmail_id!r} file_id={args.file_id!r} "
        f"(value elided)"
    )


def _cmd_get(args):
    got = lookup_password(
        gmail_id=args.gmail_id, file_id=args.file_id, path=args.path,
    )
    if got is None:
        print("[MISS] no entry")
        sys.exit(1)
    if args.show_value:
        print(f"[HIT] {got}")
    else:
        print(f"[HIT] gmail_id={args.gmail_id!r} file_id={args.file_id!r} "
              f"(value elided; pass --show-value to reveal)")


def _cmd_list(args):
    entries = list_entries(path=args.path)
    if not entries:
        print("[EMPTY] no per-file password entries configured")
        return
    print(f"[LIST] {len(entries)} entrie(s):")
    for k in sorted(entries):
        if args.show_values:
            print(f"  {k} = {entries[k]}")
        else:
            print(f"  {k}   (value elided)")


def _cmd_remove(args):
    ok = remove_password(
        gmail_id=args.gmail_id, file_id=args.file_id, path=args.path,
    )
    if ok:
        print(f"[REMOVED] gmail_id={args.gmail_id!r} file_id={args.file_id!r}")
    else:
        print(f"[MISS] nothing to remove")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="ak-nyd per-file PDF password store CLI."
    )
    parser.add_argument("--path", default=None,
                        help="Explicit JSON path (default: "
                             "$AK_PER_FILE_PASSWORDS_PATH or "
                             "./per_file_passwords.json)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_add = sub.add_parser("add", help="Add/update a password entry")
    p_add.add_argument("--gmail-id", default=None)
    p_add.add_argument("--file-id", default=None)
    p_add.add_argument("--password", required=True)
    p_add.set_defaults(func=_cmd_add)

    p_get = sub.add_parser("get", help="Look up a password entry")
    p_get.add_argument("--gmail-id", default=None)
    p_get.add_argument("--file-id", default=None)
    p_get.add_argument("--show-value", action="store_true",
                       help="Print the password (default: elided)")
    p_get.set_defaults(func=_cmd_get)

    p_list = sub.add_parser("list", help="List all entries")
    p_list.add_argument("--show-values", action="store_true",
                        help="Print passwords (default: keys only)")
    p_list.set_defaults(func=_cmd_list)

    p_remove = sub.add_parser("remove", help="Remove an entry")
    p_remove.add_argument("--gmail-id", default=None)
    p_remove.add_argument("--file-id", default=None)
    p_remove.set_defaults(func=_cmd_remove)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
