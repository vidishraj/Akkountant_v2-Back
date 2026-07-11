"""ak-nyd: per-file PDF password override.

Some statement PDFs are password-protected with a value that doesn't
match the bank's standard personal-info strategy (DOB, PAN, phone
segments, etc.). F6 (Jan-2026 HDFC, recovered via
recover_pdf_ak32o.py) is such a file — Overseer set a custom
password on it. Without a per-file override, mailProcessor's
_try_personal_info_passwords loop can't unlock it and the whole
re-parse falls over.

DESIGN (MVP per Lead's dispatch hq-wisp-6n526p — "simpler MVP first,
proper store later"):

  A JSON-backed dict keyed on gmail_id (matches reprocess_pdf's
  parameter shape) and/or fileID. Values are the plaintext
  password strings. Loaded on demand, cached per process. No DB /
  no schema change.

  Path resolution order:
    1. `AK_PER_FILE_PASSWORDS_PATH` env var if set.
    2. `<cwd>/per_file_passwords.json` if it exists.
    3. Empty dict (safe default — lookups return None).

  The JSON shape is a flat map:
    {
      "gmail_id:<gmail_message_id>": "password-string",
      "file_id:<fileID>":            "password-string"
    }
  Two namespaces so a caller with either key can hit the store.

SECURITY NOTE:

Passwords land plaintext on disk — no worse than the personal-info
flow (which also uses plaintext DOB / PAN / phone segments as
password candidates), but not great. A proper encrypted store is
deferred until the pipeline has a full secrets story (probably
alongside the ak-2ln umbrella work). For NOW: chmod 600 the file,
put it outside git, and treat it as short-lived recovery scaffolding.

Pure-Python (no framework deps) so it's testable without booting
flask/SQLAlchemy.
"""

from __future__ import annotations

import json
import os
from typing import Optional


_ENV_PATH_VAR = "AK_PER_FILE_PASSWORDS_PATH"
_DEFAULT_FILENAME = "per_file_passwords.json"


def _resolve_config_path() -> str:
    """Return the file path to load / save the JSON store from.
    Order: env var, then <cwd>/per_file_passwords.json."""
    env_path = os.environ.get(_ENV_PATH_VAR, "").strip()
    if env_path:
        return env_path
    return os.path.join(os.getcwd(), _DEFAULT_FILENAME)


def _load_raw(path: Optional[str] = None) -> dict:
    """Load the JSON file into a dict. Returns {} on missing file or
    parse error (defensive — never blow up the caller because of a
    corrupt config)."""
    if path is None:
        path = _resolve_config_path()
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {}
        return data
    except (OSError, json.JSONDecodeError):
        return {}


def _save_raw(data: dict, path: Optional[str] = None) -> None:
    """Overwrite the JSON file with `data`. Caller is responsible
    for ensuring the containing dir exists; we create it if not."""
    if path is None:
        path = _resolve_config_path()
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)
        f.write("\n")
    os.replace(tmp, path)


def lookup_password(
    gmail_id: Optional[str] = None,
    file_id: Optional[str] = None,
    *,
    path: Optional[str] = None,
) -> Optional[str]:
    """ak-nyd: return the per-file password override for a given
    gmail_id and/or fileID, or None if neither key is present.

    Preference order (both keys resolve independently; caller picks):
      1. `gmail_id:<gmail_id>` if `gmail_id` is supplied and present.
      2. `file_id:<file_id>` if `file_id` is supplied and present.

    Never raises. Missing config file → None. Corrupt config → None.
    """
    if not gmail_id and not file_id:
        return None
    data = _load_raw(path)
    if gmail_id:
        v = data.get(f"gmail_id:{gmail_id}")
        if v:
            return v
    if file_id:
        v = data.get(f"file_id:{file_id}")
        if v:
            return v
    return None


def add_password(
    password: str,
    *,
    gmail_id: Optional[str] = None,
    file_id: Optional[str] = None,
    path: Optional[str] = None,
) -> None:
    """ak-nyd: upsert a password entry. At least one of gmail_id /
    file_id must be provided. Both are written if both are supplied
    (safe for callers with only one identifier)."""
    if not gmail_id and not file_id:
        raise ValueError(
            "add_password: at least one of gmail_id or file_id required"
        )
    if not password:
        raise ValueError("add_password: password must be non-empty")
    data = _load_raw(path)
    if gmail_id:
        data[f"gmail_id:{gmail_id}"] = password
    if file_id:
        data[f"file_id:{file_id}"] = password
    _save_raw(data, path)


def remove_password(
    *,
    gmail_id: Optional[str] = None,
    file_id: Optional[str] = None,
    path: Optional[str] = None,
) -> bool:
    """Remove an entry. Returns True if anything was removed."""
    if not gmail_id and not file_id:
        return False
    data = _load_raw(path)
    removed = False
    if gmail_id:
        removed |= (data.pop(f"gmail_id:{gmail_id}", None) is not None)
    if file_id:
        removed |= (data.pop(f"file_id:{file_id}", None) is not None)
    if removed:
        _save_raw(data, path)
    return removed


def list_entries(path: Optional[str] = None) -> dict:
    """Return a shallow copy of the current entries. Useful for
    audit / CLI listing."""
    return dict(_load_raw(path))
