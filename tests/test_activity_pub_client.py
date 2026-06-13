"""Tests for ActivityPubClient — async signed GET, parse, raise, follow redirects.

Uses httpx.MockTransport so nothing touches the network: the mock handler
receives the actual outgoing httpx.Request, which lets us both verify the
signature that goes on the wire and drive every response scenario
deterministically. The client is async (httpx.AsyncClient), so get() is awaited;
the mock handlers stay sync (MockTransport supports that under AsyncClient).

Three FixedWindowCounters are injected: `general` (shared with WebfingerClient,
acquired for every request), `burst` (a short-window per-origin cap, also
acquired for every request), and `paged` (acquired additionally when the URL
carries a `page` query param). The acquire happens per actual GET — inside the
redirect loop — keyed by origin (scheme://host). The stricter-first contract
still holds (a paged request acquires `paged` before `general`); these tests do
NOT pin where `burst` sits in that order. next_available() returns the latest
(max) of whichever gates apply, now including `burst`.

Assumed constructor (flag if different):
  ActivityPubClient(key_id, private_key_pem, general, paged, burst, transport=None)
"""

import httpx
import pytest

from pub_crawler.activity_pub_client import ActivityPubClient
from support import (
    SpyCounter,
    canonical_signing_string,
    nonblocking_counter,
    parse_signature,
    verify_signature,
)

KEY_ID = "https://crawler.pub/actor#main-key"
URL = "https://remote.example/users/alice"
ORIGIN = "https://remote.example"
PAGE_URL = "https://remote.example/users/alice/followers?page=2"


def make_client(handler, pem, general=None, paged=None, burst=None):
    return ActivityPubClient(
        KEY_ID,
        pem,
        general or nonblocking_counter(),
        paged or nonblocking_counter(),
        burst or nonblocking_counter(),
        transport=httpx.MockTransport(handler),
    )


def assert_signed(request, public_key):
    """Verify the request's Signature against its own request-target + headers."""
    parsed = parse_signature(request.headers["signature"])
    assert parsed["keyId"] == KEY_ID
    assert parsed["algorithm"] == "rsa-sha256"

    names = parsed["headers"].split()
    assert names[0] == "(request-target)"
    # Rebuild the signed header set in the declared order from what was sent.
    signed = {name: request.headers[name] for name in names[1:]}
    message = canonical_signing_string(request.method, str(request.url), signed)
    verify_signature(public_key, parsed["signature"], message)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


async def test_get_returns_parsed_json(keypair):
    pem, _ = keypair
    body = {"id": URL, "type": "Person"}

    def handler(request):
        return httpx.Response(200, json=body)

    assert await make_client(handler, pem).get(URL) == body


async def test_get_sends_a_valid_signature(keypair):
    pem, public_key = keypair
    captured = {}

    def handler(request):
        captured["request"] = request
        return httpx.Response(200, json={})

    await make_client(handler, pem).get(URL)
    request = captured["request"]

    # The signed set is exactly (request-target) + host + date + user-agent.
    parsed = parse_signature(request.headers["signature"])
    assert set(parsed["headers"].split()) == {
        "(request-target)",
        "host",
        "date",
        "user-agent",
    }
    # Accept rides along unsigned and offers AS2 (exact value/q-list is the
    # client's choice).
    assert "application/activity+json" in request.headers["accept"]
    # And what's on the wire actually verifies against the published key.
    assert_signed(request, public_key)


# ---------------------------------------------------------------------------
# get_with_headers: parsed body PLUS the response headers (get() is this,
# minus the headers). Used to read the Server header for node metadata.
# ---------------------------------------------------------------------------


async def test_get_with_headers_returns_body_and_headers(keypair):
    pem, _ = keypair
    body = {"id": URL, "type": "Person"}

    def handler(request):
        return httpx.Response(200, json=body, headers={"Server": "Mastodon"})

    result_body, headers = await make_client(handler, pem).get_with_headers(URL)

    assert result_body == body
    # Real response headers are case-insensitive, so Server reads back whatever
    # case the caller asks for.
    assert headers["server"] == "Mastodon"


async def test_get_with_headers_returns_the_final_hop_headers(keypair):
    pem, _ = keypair

    def handler(request):
        if request.url.path == "/start":
            # The redirector in front identifies as nginx...
            return httpx.Response(
                302,
                headers={"Location": "https://remote.example/real", "Server": "nginx"},
            )
        # ...but we want the software of the host that actually served the doc.
        return httpx.Response(200, json={"ok": True}, headers={"Server": "Mastodon"})

    body, headers = await make_client(handler, pem).get_with_headers(
        "https://remote.example/start"
    )

    assert body == {"ok": True}
    assert headers["server"] == "Mastodon"


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status", [400, 403, 404, 410, 500, 502, 503])
async def test_get_raises_on_http_error(keypair, status):
    pem, _ = keypair

    def handler(request):
        return httpx.Response(status, json={})

    with pytest.raises(httpx.HTTPStatusError):
        await make_client(handler, pem).get(URL)


async def test_get_raises_on_connection_error(keypair):
    pem, _ = keypair

    def handler(request):
        raise httpx.ConnectError("connection refused")

    with pytest.raises(httpx.RequestError):
        await make_client(handler, pem).get(URL)


# ---------------------------------------------------------------------------
# Redirects
# ---------------------------------------------------------------------------


async def test_follows_redirect_and_re_signs(keypair):
    pem, public_key = keypair
    final = {"id": "https://remote.example/real", "type": "Person"}
    seen = []

    def handler(request):
        seen.append(request)
        if request.url.path == "/start":
            return httpx.Response(
                302, headers={"Location": "https://remote.example/real"}
            )
        return httpx.Response(200, json=final)

    result = await make_client(handler, pem).get("https://remote.example/start")

    assert result == final
    assert len(seen) == 2
    # Both hops are independently signed for their own request-target.
    for request in seen:
        assert_signed(request, public_key)
    assert seen[1].url.path == "/real"


async def test_resolves_relative_redirect(keypair):
    pem, _ = keypair
    seen = []

    def handler(request):
        seen.append(str(request.url))
        if request.url.path == "/start":
            return httpx.Response(302, headers={"Location": "/moved"})
        return httpx.Response(200, json={"ok": True})

    result = await make_client(handler, pem).get("https://remote.example/start")

    assert result == {"ok": True}
    # Relative Location resolved against the original absolute URL.
    assert seen[1] == "https://remote.example/moved"


async def test_too_many_redirects_raises(keypair):
    pem, _ = keypair

    def handler(request):
        # Always redirect onward (distinct paths) to force the cap.
        n = int(request.url.params.get("n", "0"))
        return httpx.Response(
            302, headers={"Location": f"https://remote.example/r?n={n + 1}"}
        )

    with pytest.raises(httpx.TooManyRedirects):
        await make_client(handler, pem).get("https://remote.example/r?n=0")


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


async def test_aclose_closes_the_underlying_client(keypair):
    pem, _ = keypair

    def handler(request):
        return httpx.Response(200, json={})

    client = make_client(handler, pem)
    assert client.client.is_closed is False

    await client.aclose()

    assert client.client.is_closed is True


# ---------------------------------------------------------------------------
# Rate limiting: acquire general (and paged for ?page=) before each GET
# ---------------------------------------------------------------------------


async def test_get_acquires_burst_and_general_before_fetching(keypair):
    pem, _ = keypair
    log = []
    general = SpyCounter(log, "general")
    paged = SpyCounter(log, "paged")
    burst = SpyCounter(log, "burst")

    def handler(request):
        log.append(("fetch", str(request.url)))
        return httpx.Response(200, json={})

    await make_client(handler, pem, general=general, paged=paged, burst=burst).get(URL)

    # Plain (non-paged) URL spends burst + general (once each, keyed by origin),
    # never paged. Both acquires land before the GET; their order relative to one
    # another isn't pinned.
    assert general.calls == [ORIGIN]
    assert burst.calls == [ORIGIN]
    assert paged.calls == []
    assert log[-1] == ("fetch", URL)
    assert ("burst", ORIGIN) in log[:-1]
    assert ("general", ORIGIN) in log[:-1]


async def test_paged_url_acquires_paged_general_and_burst(keypair):
    pem, _ = keypair
    log = []
    general = SpyCounter(log, "general")
    paged = SpyCounter(log, "paged")
    burst = SpyCounter(log, "burst")

    def handler(request):
        log.append(("fetch", str(request.url)))
        return httpx.Response(200, json={})

    await make_client(handler, pem, general=general, paged=paged, burst=burst).get(
        PAGE_URL
    )

    # ?page= request spends from all three, once each, keyed by origin. The
    # stricter-first contract holds (paged before general); burst's slot is not
    # pinned. All acquires precede the fetch.
    assert general.calls == [ORIGIN]
    assert paged.calls == [ORIGIN]
    assert burst.calls == [ORIGIN]
    assert log[-1] == ("fetch", PAGE_URL)
    labels = [entry[0] for entry in log[:-1]]
    assert labels.index("paged") < labels.index("general")
    assert "burst" in labels


async def test_acquires_once_per_redirect_hop(keypair):
    pem, _ = keypair
    general = SpyCounter()
    burst = SpyCounter()

    def handler(request):
        if request.url.path == "/start":
            return httpx.Response(
                302, headers={"Location": "https://remote.example/real"}
            )
        return httpx.Response(200, json={"ok": True})

    await make_client(handler, pem, general=general, burst=burst).get(
        "https://remote.example/start"
    )

    # The acquire lives in _get(), so each hop (original + redirect target)
    # acquires every gate independently.
    assert general.calls == [ORIGIN, ORIGIN]
    assert burst.calls == [ORIGIN, ORIGIN]


# ---------------------------------------------------------------------------
# next_available(url): when this client can next fetch the url (no consume)
# ---------------------------------------------------------------------------


class FakeCounter:
    """Records the origin passed to next_available; returns a fixed answer."""

    def __init__(self, result):
        self.result = result
        self.origins = []

    def next_available(self, origin):
        self.origins.append(origin)
        return self.result


def na_client(general, paged, burst):
    handler = lambda request: httpx.Response(200, json={})  # never called here
    return ActivityPubClient(
        KEY_ID, "pem", general, paged, burst, transport=httpx.MockTransport(handler)
    )


def test_next_available_non_paged_maxes_general_and_burst():
    general = FakeCounter(100)
    paged = FakeCounter(500)
    burst = FakeCounter(300)

    result = na_client(general, paged, burst).next_available(URL)  # no ?page=

    # Non-paged spends general + burst -> the later of those two; paged is
    # irrelevant to a plain GET.
    assert result == 300  # max(general=100, burst=300)
    assert general.origins == [ORIGIN]
    assert burst.origins == [ORIGIN]
    assert paged.origins == []


def test_next_available_paged_returns_the_latest_gate_paged_binding():
    general = FakeCounter(100)
    paged = FakeCounter(500)
    burst = FakeCounter(300)

    result = na_client(general, paged, burst).next_available(PAGE_URL)  # ?page=2

    # A paged request needs all three tokens -> can't go until the latest opens.
    assert result == 500  # max(general=100, paged=500, burst=300)
    assert general.origins == [ORIGIN]
    assert paged.origins == [ORIGIN]
    assert burst.origins == [ORIGIN]


def test_next_available_paged_returns_the_latest_gate_general_binding():
    # max() must pick general when it's the binding one.
    general = FakeCounter(900)
    paged = FakeCounter(300)
    burst = FakeCounter(300)

    assert na_client(general, paged, burst).next_available(PAGE_URL) == 900


def test_next_available_returns_burst_when_it_is_the_binding_gate():
    # The short-window burst cap can be the latest gate on a rapid run.
    general = FakeCounter(100)
    paged = FakeCounter(200)
    burst = FakeCounter(999)

    assert na_client(general, paged, burst).next_available(PAGE_URL) == 999
