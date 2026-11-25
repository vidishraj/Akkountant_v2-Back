"""HDFC Smart Statement URL gate, shared by every code path that fetches
from the Smart Statement portal (ak-ptxq, hoisted by ak-tspe, hardened by
ak-x5l9).

* ``canonical_hdfc_url``: the ONLY gate between a URL taken from an email
  (attacker-controllable: a phishing mail can copy HDFC's styling) and an
  HTTP request. Returns a URL REBUILT from the checked components
  (``https://<allowlisted host><path>[?<query>]``) or None. Callers must
  send the request to the returned URL, never to the original string.
* ``is_hdfc_smart_statement_url``: boolean form of the same check.
* ``follow_hdfc_redirects``: callers send with ``allow_redirects=False``
  and pass the response here. Redirects are followed only to allowlisted
  hosts (canonicalised) and always re-issued as a body-less GET, so a
  credentialed POST (Customer ID password) is never re-sent elsewhere.

ak-x5l9 (P0): ``urlparse`` and requests/urllib3 disagreed on where the
authority ends. ``https://evil.com\\@smartstatements.hdfcbank.com/x`` has
urlparse hostname = the HDFC host, but urllib3 stops the authority at the
backslash and CONNECTS TO evil.com. So the gate now:
  * rejects backslashes and any ASCII control / whitespace character;
  * requires https (http is no longer accepted);
  * rejects any userinfo (``user@``, ``:pw@``, ``@``) and any port but 443;
  * requires urlparse's host == urllib3's host, and that host allowlisted;
  * hands back a canonical URL rebuilt from those components, so the
    parser that checked and the parser that connects see the same string.
Fails closed: any parse error returns None / False.
"""
from __future__ import annotations

from urllib.parse import urljoin, urlsplit

HDFC_SMART_STATEMENT_HOSTS = frozenset({
    "smartstatements.hdfcbank.com",
    "smartstatements.hdfc.bank.in",
})

_REDIRECT_CODES = frozenset({301, 302, 303, 307, 308})
MAX_REDIRECTS = 5


def _has_forbidden_chars(url: str) -> bool:
    return "\\" in url or any(ord(c) < 0x21 or ord(c) == 0x7F for c in url)


def canonical_hdfc_url(url):
    """``https://<host><path>[?<query>]`` rebuilt from the checked parts
    of ``url`` if it is an allowlisted Smart Statement URL, else None.
    Never raises."""
    if not isinstance(url, str) or not url or _has_forbidden_chars(url):
        return None
    try:
        from urllib3.util import parse_url
        p = urlsplit(url)
        u = parse_url(url)
        if (p.scheme or "").lower() != "https" or (u.scheme or "").lower() != "https":
            return None
        if p.username is not None or p.password is not None or u.auth:
            return None
        if p.port not in (None, 443) or u.port not in (None, 443):
            return None
        # m2: hostname parsing stays inside the fail-closed try.
        host = (p.hostname or "").lower()
        if host != (u.host or "").lower() or host not in HDFC_SMART_STATEMENT_HOSTS:
            return None
        path = p.path or "/"
        if not path.startswith("/"):
            return None
        return f"https://{host}{path}" + (f"?{p.query}" if p.query else "")
    except Exception:
        return None


def is_hdfc_smart_statement_url(url) -> bool:
    """True only if ``canonical_hdfc_url(url)`` accepts it."""
    return canonical_hdfc_url(url) is not None


def new_hdfc_session(user_agent=None):
    """requests.Session for the portal: ignores proxy / netrc env
    (``trust_env=False``) so credentials can't be routed or sent elsewhere."""
    import requests
    session = requests.Session()
    session.trust_env = False
    if user_agent:
        session.headers.update({"User-Agent": user_agent})
    return session


def follow_hdfc_redirects(session, response, timeout=30, max_redirects=MAX_REDIRECTS):
    """Follow ``response``'s redirect chain, allowlisted hosts only.

    Each hop is a GET to the canonicalised target with
    ``allow_redirects=False`` and no body. Returns the first non-redirect
    response, the redirect response itself if it has no Location, or None
    when a hop points off the allowlist or the chain is too long (caller
    must fail closed).
    """
    hops = 0
    while response is not None and response.status_code in _REDIRECT_CODES:
        location = (getattr(response, "headers", None) or {}).get("Location")
        if not location:
            return response
        if not isinstance(location, str) or _has_forbidden_chars(location):
            return None
        target = canonical_hdfc_url(urljoin(getattr(response, "url", "") or "", location))
        if target is None:
            return None
        hops += 1
        if hops > max_redirects:
            return None
        response = session.get(target, timeout=timeout, allow_redirects=False)
    return response
