"""Unit tests for Kite access-token expiry handling (ak-w4p Part A).

Background — the bug these pin:
  Kite access tokens expire at 6 AM IST the day after they are issued
  ("will expire at 6 AM on the next day (regulatory requirement)",
  https://kite.trade/docs/connect/v3/user/#login-flow), and Zerodha only
  hands refresh tokens to approved platforms, which this app is not. The
  token store nevertheless wrote `expiry=0` with the comment "Kite access
  tokens don't expire", so a dead token was handed to Kite forever and the
  1-7 AM IST instrument job failed opaquely against the 6 AM boundary.

These are the first tests to exercise KiteService at all. `kiteconnect` is
not installed in CI, so (as in test_nse_patch) a fake module is installed
into sys.modules BEFORE importing the service.

Coverage:
  1. Expiry lands on the NEXT 6 AM IST boundary, never "now + 24h"
  2. Early-morning tokens (issued 01:00 IST) expire the SAME morning
  3. Tokens issued after 06:00 IST expire the following morning
  4. DST-free IST offset is respected (epoch maths, not naive local time)
  5. `expiry=0` legacy rows are treated as EXPIRED, not as never-expiring
  6. A live token is not treated as expired
  7. `_save_user_access_token` writes a real expiry on the INSERT path
  8. ...and on the UPDATE path (the original bug: it only touched
     access_token, so a re-login kept the stale expiry)
  9. An expired stored token raises KiteTokenExpired and never calls Kite
 10. A Kite-side TokenException is converted to KiteTokenExpired
 11. ...and zeroes the stored expiry so later calls fail fast
 12. get_token_status distinguishes never-connected / expired / live
 13. check_token_liveness returns False rather than raising
"""
import datetime
import sys
import types
from unittest.mock import MagicMock

import pytest


# ── Fake kiteconnect, installed before importing the service ────────────────

class _FakeTokenException(Exception):
    """Stand-in for kiteconnect.exceptions.TokenException."""


def _install_fake_kiteconnect():
    exceptions = types.ModuleType("kiteconnect.exceptions")
    exceptions.TokenException = _FakeTokenException

    lib = types.ModuleType("kiteconnect")
    lib.KiteConnect = MagicMock(name="KiteConnect")
    lib.exceptions = exceptions

    sys.modules["kiteconnect"] = lib
    sys.modules["kiteconnect.exceptions"] = exceptions
    _INSTALLED_MODULES.extend(["kiteconnect.exceptions", "kiteconnect"])


#: Third-party packages pulled in transitively by BaseService (the Google API
#: client stack and an HTML parser) that are absent in CI and irrelevant to
#: token expiry. Stubbed rather than installed so this suite stays fast and
#: dependency-free.
_STUBBED_PACKAGES = (
    "google.oauth2.credentials",
    "google.auth.transport.requests",
    "google.auth.exceptions",
    "googleapiclient.discovery",
    "googleapiclient.http",
    "googleapiclient.errors",
    "google_auth_oauthlib.flow",
    "bs4",
)

#: Names used in `except` clauses must be real exception classes, not mocks.
_STUBBED_EXCEPTIONS = {
    "google.auth.exceptions": ("RefreshError",),
    "googleapiclient.errors": ("HttpError",),
}


#: Everything this module injected into sys.modules, so it can be withdrawn
#: again -- see _withdraw_stubs().
_INSTALLED_MODULES = []
#: (parent_module, attribute) pairs we bound onto packages that already existed.
_INSTALLED_ATTRS = []


def _install_fake_module(name):
    """Register a stub module (and its parent packages) in sys.modules.

    Unknown attributes resolve to MagicMocks via module __getattr__, so we do
    not have to enumerate every symbol these packages export.
    """
    if name in sys.modules:
        return sys.modules[name]

    module = types.ModuleType(name)
    # Mark as a package so `from a.b.c import x` keeps resolving through it.
    module.__path__ = []
    module.__getattr__ = lambda attr: MagicMock(name=f"{name}.{attr}")
    for exc_name in _STUBBED_EXCEPTIONS.get(name, ()):
        setattr(module, exc_name, type(exc_name, (Exception,), {}))
    sys.modules[name] = module
    _INSTALLED_MODULES.append(name)

    # Bind onto the parent package so `import a.b` style access resolves.
    if "." in name:
        parent_name, _, child = name.rpartition(".")
        parent = _install_fake_module(parent_name)
        if not hasattr(parent, child):
            _INSTALLED_ATTRS.append((parent, child))
        setattr(parent, child, module)
    return module


def _install_import_stubs():
    for package in _STUBBED_PACKAGES:
        _install_fake_module(package)


def _withdraw_stubs(imported_under_stubs=()):
    """Undo the stubbing once the imports that needed it are done.

    Two kinds of leakage have to be reversed, or this module silently changes
    how *other* suites behave in the same pytest session:

      1. The stub packages themselves -- otherwise suites that legitimately
         skip when `bs4` / `claude_agent_sdk` / the Google stack is absent
         would import a MagicMock and run (or fail) instead.
      2. The project modules we imported *through* those stubs (utils.* and
         services.*). Left in sys.modules they act as a cache that masks the
         very ImportError other suites use to decide whether to skip.

    `models.*` is deliberately NOT withdrawn: those modules define SQLAlchemy
    tables against a shared Base, so re-importing them would redefine tables
    that are already registered. Everything this module needs is already bound
    to a local name, so dropping the cache entries is safe here.
    """
    for name in reversed(_INSTALLED_MODULES):
        sys.modules.pop(name, None)
    for parent, attr in reversed(_INSTALLED_ATTRS):
        try:
            delattr(parent, attr)
        except AttributeError:
            pass
    _INSTALLED_MODULES.clear()
    _INSTALLED_ATTRS.clear()

    for name in list(sys.modules):
        if name in imported_under_stubs:
            sys.modules.pop(name, None)


def _project_modules_loaded():
    """utils.*/services.* currently in sys.modules -- candidates for withdrawal."""
    return {
        name for name in sys.modules
        if name.split(".")[0] in ("utils", "services")
    }


_modules_before = _project_modules_loaded()


_install_fake_kiteconnect()
_install_import_stubs()

# Constructing a UserToken configures the whole SQLAlchemy registry, so every
# model the User mapper points at must be imported first. `models/__init__`
# omits investmentHistory, so it is pulled in explicitly.
import models  # noqa: E402,F401
import models.investmentHistory  # noqa: E402,F401
from services.KiteService import (  # noqa: E402
    KiteAuthError,
    KiteService,
    KiteTokenExpired,
    next_kite_token_expiry,
)
from utils.DateTimeUtil import IST  # noqa: E402

# Imports are done; take the stubs (and the modules loaded through them) back
# out so they cannot leak into other test modules in the same pytest session.
_withdraw_stubs(_project_modules_loaded() - _modules_before)


def _ist(year, month, day, hour, minute=0):
    return datetime.datetime(year, month, day, hour, minute, tzinfo=IST)


def _expiry_ist(issued_at):
    """Resolve next_kite_token_expiry back into an IST datetime."""
    return datetime.datetime.fromtimestamp(next_kite_token_expiry(issued_at), IST)


# ── 1-4: expiry boundary maths ─────────────────────────────────────────────

def test_expiry_is_next_six_am_ist_not_24h_later():
    """A token issued mid-morning dies the NEXT morning at 06:00, which is
    ~22h later -- not a flat 24h."""
    issued = _ist(2026, 9, 4, 8, 30)
    assert _expiry_ist(issued) == _ist(2026, 9, 5, 6, 0)


def test_early_morning_token_expires_the_same_morning():
    """The case that broke the 1-7 AM scheduler window: a token issued at
    01:00 IST is only valid for five hours, NOT until the next day.

    A `now + 24h` implementation would wrongly report this token as live
    until 01:00 the following day, and the instrument job would keep
    handing a dead token to Kite between 06:00 and 07:00.
    """
    issued = _ist(2026, 9, 4, 1, 0)
    assert _expiry_ist(issued) == _ist(2026, 9, 4, 6, 0)


def test_token_issued_after_six_am_survives_until_next_day():
    issued = _ist(2026, 9, 4, 6, 1)
    assert _expiry_ist(issued) == _ist(2026, 9, 5, 6, 0)


def test_expiry_exactly_at_six_am_rolls_to_next_day():
    """06:00:00 sharp is already the expiry instant, so the usable window is
    the following day's boundary."""
    issued = _ist(2026, 9, 4, 6, 0)
    assert _expiry_ist(issued) == _ist(2026, 9, 5, 6, 0)


def test_expiry_is_computed_in_ist_regardless_of_caller_timezone():
    """20:00 UTC on 4 Sep is 01:30 IST on 5 Sep, so the token expires at
    06:00 IST that same (IST) morning."""
    issued_utc = datetime.datetime(2026, 9, 4, 20, 0, tzinfo=datetime.timezone.utc)
    assert _expiry_ist(issued_utc) == _ist(2026, 9, 5, 6, 0)


# ── 5-6: legacy rows and the expiry predicate ──────────────────────────────

def test_legacy_zero_expiry_is_treated_as_expired():
    """Rows written by the pre-fix code claimed expiry=0 ("never expires").

    We cannot know when such a token was issued, so it must be treated as
    dead and force one reconnect -- the alternative is handing a probably-
    dead token to Kite indefinitely, which is the original bug.
    """
    token = types.SimpleNamespace(expiry=0, access_token="stale")
    assert KiteService._is_token_expired(token) is True


def test_past_expiry_is_expired_and_future_is_not():
    now = _ist(2026, 9, 4, 9, 0).timestamp()
    past = types.SimpleNamespace(expiry=int(now) - 1, access_token="t")
    future = types.SimpleNamespace(expiry=int(now) + 3600, access_token="t")

    assert KiteService._is_token_expired(past, now_epoch=now) is True
    assert KiteService._is_token_expired(future, now_epoch=now) is False


# ── Service harness ────────────────────────────────────────────────────────

class _TestKiteService(KiteService):
    """KiteService with a stub session in place of BaseService.db.

    `BaseService.db` is a read-only property that reaches for a Flask app
    context, so it is overridden here rather than assigned. `__init__` is
    deliberately not called: it would build a real SDK client from env vars.
    """

    def __init__(self, session):
        self.logger = MagicMock()
        self.api_key = "test_api_key"
        self.api_secret = "test_api_secret"
        self.kite = MagicMock()
        self._stub_db = types.SimpleNamespace(session=session)

    @property
    def db(self):
        return self._stub_db


def _service(stored_token=None):
    """A KiteService whose token lookup returns ``stored_token``."""
    query = MagicMock()
    query.filter_by.return_value.first.return_value = stored_token
    session = MagicMock()
    session.query.return_value = query

    svc = _TestKiteService(session)
    svc._added = session.add
    return svc


def _live_token():
    return types.SimpleNamespace(
        access_token="live-token",
        expiry=int(datetime.datetime.now(IST).timestamp()) + 3600,
        client_id="old",
    )


def _dead_token():
    return types.SimpleNamespace(
        access_token="dead-token",
        expiry=int(datetime.datetime.now(IST).timestamp()) - 3600,
        client_id="old",
    )


# ── 7-8: persistence writes a real expiry on BOTH paths ────────────────────

def test_save_token_writes_real_expiry_on_insert():
    svc = _service(stored_token=None)
    assert svc._save_user_access_token("user-1", "fresh-token") is True

    added = svc._added.call_args[0][0]
    assert added.access_token == "fresh-token"
    assert added.expiry == pytest.approx(next_kite_token_expiry(), abs=5)
    assert added.expiry > 0, "expiry=0 was the original bug"


def test_save_token_refreshes_expiry_on_update_path():
    """The original bug: the existing-row branch only assigned access_token.

    A day-2 reconnect therefore stored a brand-new token against the old
    (already past) expiry, so the freshly-authenticated session still read
    as expired.
    """
    existing = _dead_token()
    stale_expiry = existing.expiry
    svc = _service(stored_token=existing)

    assert svc._save_user_access_token("user-1", "day-two-token") is True

    assert existing.access_token == "day-two-token"
    assert existing.expiry != stale_expiry
    assert existing.expiry == pytest.approx(next_kite_token_expiry(), abs=5)
    assert existing.expiry > datetime.datetime.now(IST).timestamp()


# ── 9-11: expired tokens surface as typed, reconnect-able errors ───────────

def test_expired_token_raises_and_never_calls_kite():
    svc = _service(stored_token=_dead_token())

    with pytest.raises(KiteTokenExpired):
        svc.get_holdings("user-1")

    svc.kite.holdings.assert_not_called()
    svc.kite.set_access_token.assert_not_called()


def test_missing_token_raises_kite_auth_error():
    svc = _service(stored_token=None)
    with pytest.raises(KiteAuthError):
        svc.get_holdings("user-1")


def test_live_token_is_passed_through_to_kite():
    svc = _service(stored_token=_live_token())
    svc.kite.holdings.return_value = [{"tradingsymbol": "INFY"}]

    assert svc.get_holdings("user-1") == [{"tradingsymbol": "INFY"}]
    svc.kite.set_access_token.assert_called_once_with("live-token")


def test_kite_side_token_exception_becomes_kite_token_expired():
    """Kite can reject a token before our clock says it should -- a manual
    logout or a master-logout from Kite Web. Both must reach the same
    reconnect prompt, not a bare 500."""
    token = _live_token()
    svc = _service(stored_token=token)
    svc.kite.holdings.side_effect = _FakeTokenException("token expired")

    with pytest.raises(KiteTokenExpired):
        svc.get_holdings("user-1")


def test_kite_side_token_exception_marks_stored_token_expired():
    """After Kite rejects it, the row is zeroed so subsequent calls fail
    fast locally instead of making another doomed round-trip."""
    token = _live_token()
    svc = _service(stored_token=token)
    svc.kite.holdings.side_effect = _FakeTokenException("token expired")

    with pytest.raises(KiteTokenExpired):
        svc.get_holdings("user-1")

    assert token.expiry == 0


def test_non_auth_errors_are_not_converted_to_auth_errors():
    """A network blip must stay a network blip -- turning it into
    KiteTokenExpired would tell the user to reconnect for no reason."""
    svc = _service(stored_token=_live_token())
    svc.kite.holdings.side_effect = RuntimeError("connection reset")

    with pytest.raises(RuntimeError):
        svc.get_holdings("user-1")


# ── 12-13: status reporting and the liveness probe ─────────────────────────

def test_token_status_never_connected():
    status = _service(stored_token=None).get_token_status("user-1")
    assert status["connected"] is False
    assert status["reconnect_required"] is True
    assert status["reason"] == "never_connected"


def test_token_status_expired():
    status = _service(stored_token=_dead_token()).get_token_status("user-1")
    assert status["connected"] is False
    assert status["reconnect_required"] is True
    assert status["reason"] == "expired"


def test_token_status_legacy_zero_expiry_reports_unknown():
    legacy = types.SimpleNamespace(access_token="t", expiry=0, client_id="c")
    status = _service(stored_token=legacy).get_token_status("user-1")
    assert status["connected"] is False
    assert status["reason"] == "unknown_expiry"


def test_token_status_live():
    status = _service(stored_token=_live_token()).get_token_status("user-1")
    assert status["connected"] is True
    assert status["reconnect_required"] is False
    assert status["expires_at"] is not None


def test_check_token_liveness_returns_false_instead_of_raising():
    svc = _service(stored_token=_dead_token())
    assert svc.check_token_liveness("user-1") is False


def test_check_token_liveness_true_for_live_token():
    svc = _service(stored_token=_live_token())
    svc.kite.profile.return_value = {"user_id": "AB1234"}
    assert svc.check_token_liveness("user-1") is True
