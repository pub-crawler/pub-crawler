"""Tests for WebfingerClient — resolve a webfinger address to its actor id.

No signing: a single unsigned GET to the webfinger endpoint. get_actor_id
returns the actor's https URL from the JRD's self link, which the caller then
hands to the signed ActivityPubClient. The client is async (httpx.AsyncClient),
so get_actor_id is awaited; httpx.MockTransport intercepts the lookup and the
sync handlers work fine under AsyncClient.

Assumed contract (adjust the tests if the shape differs):
  WebfingerClient(transport=None).get_actor_id(wf) ->
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

from webfinger import WebfingerClient

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


def make_client(handler):
    return WebfingerClient(transport=httpx.MockTransport(handler))


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


async def test_get_actor_id_returns_the_actor_url():
    assert await make_client(serve([AP_SELF])).get_actor_id("bot@crawler.pub") == ACTOR_URL


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
    actor_id = await make_client(serve([LD_SELF, AP_SELF])).get_actor_id("bot@crawler.pub")
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
