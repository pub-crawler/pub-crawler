"""Tests for BlockAllCookiesPolicy — an http.cookiejar policy that refuses every
cookie, so the crawler's httpx client carries a permanently-empty cookie jar.

Why it exists: a single long-lived AsyncClient reused across tens of thousands of
hosts accumulates a cookie per host, and http.cookiejar then pays O(jar size) per
request (deepvalues/set_cookie) — a CPU cost that grows without bound. ActivityPub
fetches authenticate with HTTP Signatures, never cookies, so the jar should stay
empty; this policy enforces that.

Black-box: the tests drive a real http.cookiejar.CookieJar wired with the policy and
assert the *behaviour* (nothing is ever stored or sent), not which policy methods the
implementation overrides. Each block assertion is paired with a control using the
DEFAULT policy on the identical fixture, so a passing block test can't be a false
positive from a malformed cookie.

Contract assumed (flag if different): pub_crawler.block_all_cookies_policy exposes a
class BlockAllCookiesPolicy, usable as http.cookiejar.CookieJar(BlockAllCookiesPolicy()),
whose effect is that no Set-Cookie is stored and no Cookie header is ever sent.
"""

import email.message
import http.cookiejar
import urllib.request

from pub_crawler.block_all_cookies_policy import BlockAllCookiesPolicy

URL = "https://mastodon.example/users/alice"
SET_COOKIE = "session=abc; Path=/"


class _Response:
    """Minimal stand-in for what http.cookiejar reads off a response: just .info()
    returning the headers, which is where Set-Cookie lives."""

    def __init__(self, *set_cookie_values):
        self._headers = email.message.Message()
        for value in set_cookie_values:
            self._headers["Set-Cookie"] = value

    def info(self):
        return self._headers


def _request():
    return urllib.request.Request(URL)


def _extracted_cookie():
    """A genuine Cookie object, parsed the way the real code would — by extracting
    SET_COOKIE with the permissive default policy."""
    src = http.cookiejar.CookieJar()  # default (permissive) policy
    src.extract_cookies(_Response(SET_COOKIE), _request())
    return list(src)[0]


# --- storage: a Set-Cookie response must leave the jar empty --------------------


def test_default_policy_would_store_the_cookie():
    # Control: proves the fixture is a real, storable cookie, so the block test below
    # is meaningful (an empty jar there = policy rejection, not a malformed cookie).
    jar = http.cookiejar.CookieJar()
    jar.extract_cookies(_Response(SET_COOKIE), _request())
    assert len(jar) == 1


def test_block_all_stores_no_cookies_from_a_response():
    jar = http.cookiejar.CookieJar(BlockAllCookiesPolicy())
    jar.extract_cookies(_Response(SET_COOKIE), _request())
    assert len(jar) == 0


# --- sending: even a cookie already in the jar must not be sent -----------------


def test_default_policy_would_send_the_cookie():
    # Control for the send path.
    jar = http.cookiejar.CookieJar()
    jar.set_cookie(_extracted_cookie())
    req = _request()
    jar.add_cookie_header(req)
    assert req.get_header("Cookie") is not None


def test_block_all_sends_no_cookie_even_if_one_is_present():
    jar = http.cookiejar.CookieJar(BlockAllCookiesPolicy())
    jar.set_cookie(_extracted_cookie())  # injected directly, bypassing set_ok
    assert len(jar) == 1  # it IS in the jar...
    req = _request()
    jar.add_cookie_header(req)
    assert req.get_header("Cookie") is None  # ...but the policy refuses to send it


# --- the policy gates, directly -------------------------------------------------


def test_every_policy_gate_rejects():
    policy = BlockAllCookiesPolicy()
    req = _request()
    cookie = _extracted_cookie()
    assert policy.set_ok(cookie, req) is False  # never store
    assert policy.return_ok(cookie, req) is False  # never send
    assert policy.domain_return_ok("mastodon.example", req) is False  # skip domain
    assert policy.path_return_ok("/", req) is False  # skip path
