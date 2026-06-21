"""Tests for WebfingerClient — resolve a webfinger address to its actor id.

No signing: a single unsigned GET to the webfinger endpoint. get_actor_id
returns the actor's https URL from the JRD's self link, which the caller then
hands to the signed ActivityPubClient. The client is async (httpx.AsyncClient),
so get_actor_id is awaited; httpx.MockTransport intercepts the lookup and the
sync handlers work fine under AsyncClient.

Two FixedWindowCounters are injected, both shared with ActivityPubClient (same
per-IP budget): `general` and a short-window `burst` cap. Both are acquired
before each lookup, keyed by origin (scheme://host); the tests don't pin their
relative order. next_available() returns the later (max) of the two.

Assumed contract (adjust the tests if the shape differs):
  WebfingerClient(general, burst, transport=None).get_actor_id(wf) ->
    GET https://{host}/.well-known/webfinger?resource=acct:{user}@{host}
    choose the self link by preference:
      1. type == application/activity+json
      2. else type application/ld+json carrying the AS2 profile
      3. else give up -> raise ValueError
    return that link's href
  Address accepted as user@host, acct:user@host, or @user@host.
"""

import httpx
import pytest

from pub_crawler.webfinger_client import WebfingerClient
from support import SpyCounter, nonblocking_counter

ACTOR_URL = "https://crawler.pub/actor"
LD_URL = "https://crawler.pub/actor-ld"

AP_SELF = {"rel": "self", "type": "application/activity+json", "href": ACTOR_URL}
LD_SELF = {
    "rel": "self",
    "type": 'application/ld+json; profile="https://www.w3.org/ns/activitystreams"',
    "href": LD_URL,
}
PROFILE_PAGE = {
    "rel": "http://webfinger.net/rel/profile-page",
    "type": "text/html",
    "href": "https://crawler.pub/",
}


def serve(links, seen=None):
    """Handler that serves a JRD with the given links, optionally recording it."""

    def handler(request):
        if seen is not None:
            seen["webfinger"] = request
        return httpx.Response(
            200, json={"subject": "acct:bot@crawler.pub", "links": links}
        )

    return handler


def make_client(handler, general=None, burst=None):
    return WebfingerClient(
        general or nonblocking_counter(),
        burst or nonblocking_counter(),
        transport=httpx.MockTransport(handler),
    )


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


async def test_get_actor_id_returns_the_actor_url():
    assert (
        await make_client(serve([AP_SELF])).get_actor_id("bot@crawler.pub") == ACTOR_URL
    )


async def test_queries_the_webfinger_endpoint_over_https():
    seen = {}
    await make_client(serve([AP_SELF], seen)).get_actor_id("bot@crawler.pub")

    wf = seen["webfinger"]
    assert wf.url.scheme == "https"
    assert wf.url.host == "crawler.pub"
    assert wf.url.path == "/.well-known/webfinger"
    assert wf.url.params["resource"] == "acct:bot@crawler.pub"


@pytest.mark.parametrize(
    "wf", ["bot@crawler.pub", "acct:bot@crawler.pub", "@bot@crawler.pub"]
)
async def test_accepts_common_address_forms(wf):
    seen = {}
    actor_id = await make_client(serve([AP_SELF], seen)).get_actor_id(wf)

    assert actor_id == ACTOR_URL
    assert seen["webfinger"].url.host == "crawler.pub"
    assert seen["webfinger"].url.params["resource"] == "acct:bot@crawler.pub"


# ---------------------------------------------------------------------------
# Self-link preference: activity+json > ld+json(+profile) > give up
# ---------------------------------------------------------------------------


async def test_prefers_activity_json_over_ld_json():
    # Both present (ld+json listed first to prove preference, not order).
    actor_id = await make_client(serve([LD_SELF, AP_SELF])).get_actor_id(
        "bot@crawler.pub"
    )
    assert actor_id == ACTOR_URL


async def test_falls_back_to_ld_json_with_profile():
    # No activity+json link available.
    actor_id = await make_client(serve([LD_SELF, PROFILE_PAGE])).get_actor_id(
        "bot@crawler.pub"
    )
    assert actor_id == LD_URL


async def test_gives_up_when_no_activitypub_self_link():
    # Only a non-AP link (HTML profile page) — nothing fetchable as an actor.
    with pytest.raises(ValueError):
        await make_client(serve([PROFILE_PAGE])).get_actor_id("bot@crawler.pub")


# ---------------------------------------------------------------------------
# Missing account
# ---------------------------------------------------------------------------


async def test_unknown_account_raises_http_error():
    def handler(request):
        # webfinger reports no such account
        return httpx.Response(404, json={})

    with pytest.raises(httpx.HTTPStatusError):
        await make_client(handler).get_actor_id("nobody@crawler.pub")


# ---------------------------------------------------------------------------
# Rate limiting: acquire the shared general counter before fetching
# ---------------------------------------------------------------------------


async def test_acquires_burst_and_general_before_fetching():
    log = []
    general = SpyCounter(log, "general")
    burst = SpyCounter(log, "burst")

    def handler(request):
        log.append(("fetch", str(request.url)))
        return httpx.Response(
            200, json={"subject": "acct:bot@crawler.pub", "links": [AP_SELF]}
        )

    client = WebfingerClient(general, burst, transport=httpx.MockTransport(handler))
    await client.get_actor_id("bot@crawler.pub")

    # Both counters acquired once, keyed by origin, before the GET; relative
    # order isn't pinned.
    assert general.calls == ["https://crawler.pub"]
    assert burst.calls == ["https://crawler.pub"]
    assert log[-1][0] == "fetch"
    assert ("general", "https://crawler.pub") in log[:-1]
    assert ("burst", "https://crawler.pub") in log[:-1]


# ---------------------------------------------------------------------------
# Constructor accepts max_workers (used to size the connection pool)
# ---------------------------------------------------------------------------


async def test_constructor_accepts_max_workers():
    client = WebfingerClient(
        nonblocking_counter(),
        nonblocking_counter(),
        transport=httpx.MockTransport(serve([AP_SELF])),
        max_workers=12,
    )
    assert await client.get_actor_id("bot@crawler.pub") == ACTOR_URL


# ---------------------------------------------------------------------------
# next_available(webfinger): when the host's general budget next allows a lookup
# ---------------------------------------------------------------------------


class FakeCounter:
    """Records the origin passed to next_available; returns a fixed answer."""

    def __init__(self, result):
        self.result = result
        self.origins = []

    def next_available(self, origin):
        self.origins.append(origin)
        return self.result


def counter_client(general, burst):
    handler = lambda request: httpx.Response(200, json={})  # never called here
    return WebfingerClient(general, burst, transport=httpx.MockTransport(handler))


@pytest.mark.parametrize(
    "wf", ["bot@crawler.pub", "acct:bot@crawler.pub", "@bot@crawler.pub"]
)
def test_next_available_maxes_general_and_burst_keyed_by_host(wf):
    general = FakeCounter(result=100)
    burst = FakeCounter(result=500)
    client = counter_client(general, burst)

    result = client.next_available(wf)

    # Takes a webfinger (not a URL), sync (no await). Derives the host from the
    # acct, keys both counters by that origin (scheme://host), and returns the
    # later (max) of the two answers.
    assert result == 500  # max(general=100, burst=500)
    assert general.origins == ["https://crawler.pub"]
    assert burst.origins == ["https://crawler.pub"]
