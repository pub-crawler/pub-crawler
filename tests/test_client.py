"""Tests for ActivityPubClient — async signed GET, parse, raise, follow redirects.

Uses httpx.MockTransport so nothing touches the network: the mock handler
receives the actual outgoing httpx.Request, which lets us both verify the
signature that goes on the wire and drive every response scenario
deterministically. The client is async (httpx.AsyncClient), so get() is awaited;
the mock handlers stay sync (MockTransport supports that under AsyncClient).
"""

import httpx
import pytest

from client import ActivityPubClient
from support import canonical_signing_string, parse_signature, verify_signature

KEY_ID = "https://crawler.pub/actor#main-key"
URL = "https://remote.example/users/alice"


def make_client(handler, pem):
    return ActivityPubClient(KEY_ID, pem, transport=httpx.MockTransport(handler))


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
