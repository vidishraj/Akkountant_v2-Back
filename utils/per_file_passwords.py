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

  Path resolution order (ak-nyd v2 — reviewer BOUNCE hq-wisp-*):
    1. `AK_PER_FILE_PASSWORDS_PATH` env var if set.
    2. `~/.config/akkountant/per_file_passwords.json`
       (XDG-style config dir OUTSIDE the repo — v1 defaulted to
       CWD which is a landmine).
    3. Empty dict (safe default — lookups return None).

  The JSON shape is a flat map:
    {
      "gmail_id:<gmail_message_id>": "password-string",
      "file_id:<fileID>":            "password-string"
    }
  Two namespaces so a caller with either key can hit the store.

SECURITY (ak-nyd v2 — reviewer flagged three landmines in v1; all
three fixed here):

  1. **0o600 mode on write.** _save_raw runs `os.chmod(path, 0o600)`
     after every write so only the owning user can read/write.
     Umask can't override — chmod is explicit. Parent dir is 0o700
     when we create it.

  2. **Default path outside the repo.** v1 landed
     `./per_file_passwords.json` next to the source tree with git
     watching. v2 default is
     `~/.config/akkountant/per_file_passwords.json`.

  3. **.gitignore entry.** The backend .gitignore blocks
     `per_file_passwords.json` at any depth so an operator running
     an old (pre-v2) CLI can't accidentally commit the file.

  Deferred: proper encrypted-secrets store (probably alongside the
  ak-2ln umbrella work). Passwords remain plaintext even under this
  v2 hardening — the fixes above just narrow the exposure surface.

Pure-Python (no framework deps) so it's testable without booting
flask/SQLAlchemy.
"""

from __future__ import annotations

import json
import os
from typing import Optional


_ENV_PATH_VAR = "AK_PER_FILE_PASSWORDS_PATH"
_DEFAULT_FILENAME = "per_file_passwords.json"

# ak-nyd v2: default lives OUTSIDE the repo. XDG-style config dir
# under $HOME.
_DEFAULT_CONFIG_DIR_REL = os.path.join(".config", "akkountant")

# ak-nyd v2 file / dir modes.
_FILE_MODE = 0o600  # owner rw only
_DIR_MODE = 0o700   # owner rwx only


def _default_config_path() -> str:
    """ak-nyd v2: resolve the OUT-OF-REPO default path.
    Uses os.path.expanduser("~") so the path picks up the running
    user's home dir."""
    home = os.path.expanduser("~")
    return os.path.join(home, _DEFAULT_CONFIG_DIR_REL, _DEFAULT_FILENAME)


def _resolve_config_path() -> str:
    """Return the file path to load / save the JSON store from.
    Order: env var, then ak-nyd v2 default outside the repo."""
    env_path = os.environ.get(_ENV_PATH_VAR, "").strip()
    if env_path:
        return env_path
    return _default_config_path()


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
    """Overwrite the JSON file with `data`. Enforces ak-nyd v2 file /
    dir modes (0o600 on the file, 0o700 on the parent dir when we
    create it).

    Sequence:
      1. Create parent dir if missing (0o700; won't tighten an
         existing dir the operator manages).
      2. Write to a `.tmp` sibling.
      3. chmod the tmp to 0o600 BEFORE the rename — closes a
         narrow race where a reader could open the file between
         rename and chmod.
      4. Atomic rename to the target path.
      5. Belt-and-braces chmod of the final path — some
         filesystems (or umask quirks) can drop the mode across
         rename; explicit chmod covers those.
    """
    if path is None:
        path = _resolve_config_path()
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, mode=_DIR_MODE, exist_ok=True)
        # os.makedirs honors mode only when creating — cascade a
        # chmod on the final component so the mode is applied even
        # if a parent already existed with a looser mode.
        try:
            os.chmod(parent, _DIR_MODE)
        except OSError:
            pass  # non-fatal
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)
        f.write("\n")
    # ak-nyd v2: chmod BEFORE the rename so the file is 0o600 the
    # instant it becomes visible under the final path.
    try:
        os.chmod(tmp, _FILE_MODE)
    except OSError:
        pass  # non-fatal — belt-and-braces chmod below covers this
    os.replace(tmp, path)
    try:
        os.chmod(path, _FILE_MODE)
    except OSError:
        pass  # non-fatal


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
