"""Tests for main.crawl — dispatch a URL or webfinger address to a fetched object.

Exercises the wiring only (URL-vs-webfinger dispatch, that the actor fetch is
signed); the signing, fetching, and webfinger resolution themselves are covered
in their own suites. crawl is async; a single httpx.MockTransport is threaded
into both clients, and a generated key is injected so the real private.pem is
never touched.

Assumed contract:
  async crawl(id, *, transport=None, private_key_pem=None) -> fetched JSON object
  id starting with http(s):// is fetched directly; otherwise resolved via
  WebfingerClient.get_actor_id first.
"""

import httpx
import pytest

import crawl as crawl_module
from crawl import crawl
from support import canonical_signing_string, parse_signature, verify_signature

KEY_ID = "https://crawler.pub/actor#main-key"
ACTOR_URL = "https://crawler.pub/actor"
ACTOR = {"id": ACTOR_URL, "type": "Application", "preferredUsername": "bot"}
WF_JRD = {
    "subject": "acct:bot@crawler.pub",
    "links": [{"rel": "self", "type": "application/activity+json", "href": ACTOR_URL}],
}


async def test_crawl_with_url_fetches_directly(keypair):
    pem, _ = keypair
    paths = []

    def handler(request):
        paths.append(request.url.path)
        return httpx.Response(200, json=ACTOR)

    result = await crawl(
        ACTOR_URL, transport=httpx.MockTransport(handler), private_key_pem=pem
    )

    assert result == ACTOR
    # A URL is fetched straight off — no webfinger round-trip.
    assert "/.well-known/webfinger" not in paths


async def test_crawl_with_webfinger_resolves_then_fetches(keypair):
    pem, _ = keypair
    seen = {"webfinger": False}

    def handler(request):
        if request.url.path == "/.well-known/webfinger":
            seen["webfinger"] = True
            assert request.url.params["resource"] == "acct:bot@crawler.pub"
            return httpx.Response(200, json=WF_JRD)
        return httpx.Response(200, json=ACTOR)

    result = await crawl(
        "bot@crawler.pub", transport=httpx.MockTransport(handler), private_key_pem=pem
    )

    assert result == ACTOR
    assert seen["webfinger"], "expected a webfinger lookup for an @-address"


async def test_crawl_signs_the_actor_request(keypair):
    pem, public_key = keypair
    captured = {}

    def handler(request):
        if request.url.path == "/.well-known/webfinger":
            return httpx.Response(200, json=WF_JRD)
        captured["request"] = request
        return httpx.Response(200, json=ACTOR)

    await crawl(
        "bot@crawler.pub", transport=httpx.MockTransport(handler), private_key_pem=pem
    )

    request = captured["request"]
    parsed = parse_signature(request.headers["signature"])
    assert parsed["keyId"] == KEY_ID

    signed = {
        name: request.headers[name]
        for name in parsed["headers"].split()
        if name != "(request-target)"
    }
    message = canonical_signing_string(request.method, str(request.url), signed)
    verify_signature(public_key, parsed["signature"], message)


# ---------------------------------------------------------------------------
# Client lifecycle: crawl owns and closes both clients (even on error)
# ---------------------------------------------------------------------------


def make_spy_clients(created, ap_error=None):
    """Spy WebfingerClient/ActivityPubClient replacements that record aclose."""

    class SpyActivityPubClient:
        def __init__(self, *args, **kwargs):
            self.closed = False
            created.append(self)

        async def get(self, url):
            if ap_error is not None:
                raise ap_error
            return {"id": url}

        async def aclose(self):
            self.closed = True

    class SpyWebfingerClient:
        def __init__(self, *args, **kwargs):
            self.closed = False
            created.append(self)

        async def get_actor_id(self, wf):
            return ACTOR_URL

        async def aclose(self):
            self.closed = True

    return SpyActivityPubClient, SpyWebfingerClient


async def test_crawl_closes_both_clients(monkeypatch, keypair):
    pem, _ = keypair
    created = []
    spy_ap, spy_wf = make_spy_clients(created)
    monkeypatch.setattr(crawl_module, "ActivityPubClient", spy_ap)
    monkeypatch.setattr(crawl_module, "WebfingerClient", spy_wf)

    await crawl("user@remote.example", private_key_pem=pem)

    assert len(created) == 2
    assert all(client.closed for client in created)


async def test_crawl_closes_clients_even_on_error(monkeypatch, keypair):
    pem, _ = keypair
    created = []
    spy_ap, spy_wf = make_spy_clients(created, ap_error=RuntimeError("boom"))
    monkeypatch.setattr(crawl_module, "ActivityPubClient", spy_ap)
    monkeypatch.setattr(crawl_module, "WebfingerClient", spy_wf)

    with pytest.raises(RuntimeError):
        await crawl("user@remote.example", private_key_pem=pem)

    assert created
    assert all(client.closed for client in created)
