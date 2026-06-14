"""Tests for ActorHandler — fetch an actor, stamp the node, enqueue collections.

handle fetches the actor via the signed client, adds/enriches the graph node
with scalar metadata + a last_fetch_date stamp, and enqueues a `collection` job
for each of followers/following (carrying the actor's depth). It enqueues
collections UNCONDITIONALLY — every node, including leaves, gets its counts; the
depth bound that stops page-walking lives in CollectionHandler now, not here.
Dedup: a node already stamped with last_fetch_date is skipped (no fetch/enqueue).

Pure DI unit tests: a fake async client, a recording FakeDispatcher, a FakeGraph.

Assumed contract (flag if different):
  ActorHandler(client, dispatcher, graph).handle(job)
    job = {job_type:'actor', actor_id, depth}
    if node has last_fetch_date -> return (skip)
    actor = await client.get(actor_id); stamp node with scalar metadata + last_fetch_date
    enqueue a collection job for followers and following:
      {job_type:'collection', collection_id:<url>, owner_id:actor_id,
       direction:'followers'|'following', depth:<actor's depth>}
"""

import json

import httpx
import pytest

from pub_crawler.actor_handler import ActorHandler
from support import FakeDispatcher, FakeGraph

ACTOR_ID = "https://cosocial.ca/users/evan"
FOLLOWERS_URL = "https://cosocial.ca/users/evan/followers"
FOLLOWING_URL = "https://cosocial.ca/users/evan/following"
ACTOR = {
    "id": ACTOR_ID,
    "type": "Person",
    "preferredUsername": "evan",
    "name": "Evan Prodromou",
    "followers": FOLLOWERS_URL,
    "following": FOLLOWING_URL,
}


def actor_job(actor_id, depth):
    return {"job_type": "actor", "actor_id": actor_id, "depth": depth}


def collection_job(collection_id, direction, depth):
    return {
        "job_type": "collection",
        "collection_id": collection_id,
        "owner_id": ACTOR_ID,
        "direction": direction,
        "depth": depth,
    }


NA_RESULT = 4242
SERVER = "Mastodon"


def http_status_error(status):
    """The typed error httpx raises from response.raise_for_status() — carries
    the code at .response.status_code."""
    request = httpx.Request("GET", ACTOR_ID)
    return httpx.HTTPStatusError(
        f"{status}", request=request, response=httpx.Response(status, request=request)
    )


class FakeActivityPubClient:
    def __init__(self, actor=ACTOR, error=None, headers=None):
        self.actor = actor
        self.error = error
        # httpx.Headers is case-insensitive, like a real response's headers.
        self.headers = httpx.Headers({} if headers is None else headers)
        self.calls = []
        self.na_calls = []

    async def get_with_headers(self, url):
        self.calls.append(url)
        if self.error is not None:
            raise self.error
        return self.actor, self.headers

    async def get(self, url):
        json, _ = await self.get_with_headers(url)
        return json

    def next_available(self, url):
        self.na_calls.append(url)
        return NA_RESULT


def make_handler(client, graph, dispatcher):
    return ActorHandler(client, dispatcher, graph)


# ---------------------------------------------------------------------------
# Fetch + stamp
# ---------------------------------------------------------------------------


async def test_fetches_actor_and_stamps_node():
    client = FakeActivityPubClient()
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 0))

    assert client.calls == [ACTOR_ID]
    assert await graph.get_node_property(ACTOR_ID, "type") == "Person"
    assert await graph.get_node_property(ACTOR_ID, "preferred_username") == "evan"
    last_fetch_date = await graph.get_node_property(ACTOR_ID, "last_fetch_date")
    assert isinstance(last_fetch_date, str)
    assert last_fetch_date


@pytest.mark.parametrize("depth", [0, 3])
async def test_stamps_node_with_the_crawl_depth(depth):
    client = FakeActivityPubClient()
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(
        actor_job(ACTOR_ID, depth)
    )

    assert await graph.get_node_property(ACTOR_ID, "depth") == depth


@pytest.mark.parametrize("prop", ["indexable", "discoverable"])
async def test_stamps_indexable_and_discoverable_flags(prop):
    actor = {**ACTOR, "indexable": True, "discoverable": True}
    client = FakeActivityPubClient(actor=actor)
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 0))

    assert await graph.get_node_property(ACTOR_ID, prop) is True


@pytest.mark.parametrize("prop", ["indexable", "discoverable"])
async def test_stamps_indexable_and_discoverable_when_false(prop):
    # The privacy-relevant case: an explicit `false` must be recorded as False,
    # NOT dropped — a non-discoverable actor must be distinguishable from one
    # that simply omits the field.
    actor = {**ACTOR, "indexable": False, "discoverable": False}
    client = FakeActivityPubClient(actor=actor)
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 0))

    assert await graph.get_node_property(ACTOR_ID, prop) is False


@pytest.mark.parametrize("prop", ["indexable", "discoverable"])
async def test_omitted_indexable_and_discoverable_stay_absent(prop):
    # No field on the actor -> no property on the node (absent != False).
    client = FakeActivityPubClient(actor=ACTOR)  # ACTOR carries neither flag
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 0))

    assert await graph.get_node_property(ACTOR_ID, prop) is None


async def test_stamps_summary_from_the_actor_doc():
    # The bio/summary, a scalar string, copied verbatim onto the node.
    summary = "<p>Fediverse plumber.</p>"
    actor = {**ACTOR, "summary": summary}
    client = FakeActivityPubClient(actor=actor)
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 0))

    assert await graph.get_node_property(ACTOR_ID, "summary") == summary


async def test_omitted_summary_stays_absent():
    # No summary on the actor -> no property on the node.
    client = FakeActivityPubClient(actor=ACTOR)  # ACTOR carries no summary
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 0))

    assert await graph.get_node_property(ACTOR_ID, "summary") is None


# ---------------------------------------------------------------------------
# icon — mainline only: an Image object carrying a `url`. That covers the bulk
# of Fediverse implementations; every other shape (bare string, Link, list)
# stores nothing for now. The long tail can be revisited later.
# ---------------------------------------------------------------------------

ICON_URL = "https://cosocial.ca/avatars/evan.png"


async def test_stamps_icon_url_from_an_image_object():
    # The common Mastodon shape: icon is an Image object carrying the url.
    actor = {**ACTOR, "icon": {"type": "Image", "url": ICON_URL}}
    client = FakeActivityPubClient(actor=actor)
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 0))

    assert await graph.get_node_property(ACTOR_ID, "icon") == ICON_URL


@pytest.mark.parametrize(
    "icon",
    [
        ICON_URL,  # bare URL string
        {"type": "Link", "href": ICON_URL},  # Link object (url in href)
        [{"type": "Image", "url": ICON_URL}],  # list of representations
    ],
    ids=["bare-string", "link-object", "list"],
)
async def test_non_mainline_icon_shapes_store_nothing(icon):
    # Only the Image-with-url mainline is handled; other shapes are skipped.
    actor = {**ACTOR, "icon": icon}
    client = FakeActivityPubClient(actor=actor)
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 0))

    assert await graph.get_node_property(ACTOR_ID, "icon") is None


async def test_omitted_icon_stays_absent():
    # No icon on the actor -> no property on the node.
    client = FakeActivityPubClient(actor=ACTOR)  # ACTOR carries no icon
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 0))

    assert await graph.get_node_property(ACTOR_ID, "icon") is None


# ---------------------------------------------------------------------------
# properties — Mastodon's PropertyValue attachments (user "profile fields").
# Packed into a single `properties` node property: a JSON-encoded list of
# [name, value] pairs, in document order. Only type=="PropertyValue" entries
# count; other attachment types (images, hashtags) are ignored. value HTML is
# kept verbatim. No PropertyValue entries -> no property.
# ---------------------------------------------------------------------------


def property_value(name, value):
    return {"type": "PropertyValue", "name": name, "value": value}


async def test_packs_property_value_attachments_into_properties():
    pub = "<a href=\"https://cosocial.ca/@evan\">cosocial.ca/@evan</a>"
    work = "<a href=\"https://social.openearth.org/@evan\">social.openearth.org/@evan</a>"
    actor = {
        **ACTOR,
        "attachment": [property_value("Public", pub), property_value("Work", work)],
    }
    client = FakeActivityPubClient(actor=actor)
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 0))

    stored = await graph.get_node_property(ACTOR_ID, "properties")
    assert json.loads(stored) == [["Public", pub], ["Work", work]]


async def test_properties_preserve_order_and_duplicate_names():
    # Names can repeat and order is meaningful -> a list, not a dict.
    actor = {
        **ACTOR,
        "attachment": [
            property_value("Link", "one"),
            property_value("Link", "two"),
        ],
    }
    client = FakeActivityPubClient(actor=actor)
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 0))

    stored = await graph.get_node_property(ACTOR_ID, "properties")
    assert json.loads(stored) == [["Link", "one"], ["Link", "two"]]


async def test_non_property_value_attachments_are_excluded():
    # attachment can carry other types (e.g. an image) -> only PropertyValue counts.
    actor = {
        **ACTOR,
        "attachment": [
            {"type": "Image", "url": "https://example.com/pic.png"},
            property_value("Work", "here"),
        ],
    }
    client = FakeActivityPubClient(actor=actor)
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 0))

    stored = await graph.get_node_property(ACTOR_ID, "properties")
    assert json.loads(stored) == [["Work", "here"]]


@pytest.mark.parametrize(
    "incomplete",
    [
        {"type": "PropertyValue", "name": "Work"},  # missing value
        {"type": "PropertyValue", "value": "here"},  # missing name
    ],
    ids=["missing-value", "missing-name"],
)
async def test_incomplete_property_values_are_dropped(incomplete):
    # A PropertyValue lacking name or value is skipped, not stored with a null.
    actor = {
        **ACTOR,
        "attachment": [incomplete, property_value("Public", "ok")],
    }
    client = FakeActivityPubClient(actor=actor)
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 0))

    stored = await graph.get_node_property(ACTOR_ID, "properties")
    assert json.loads(stored) == [["Public", "ok"]]


async def test_omitted_attachment_leaves_properties_absent():
    client = FakeActivityPubClient(actor=ACTOR)  # ACTOR carries no attachment
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 0))

    assert await graph.get_node_property(ACTOR_ID, "properties") is None


async def test_attachment_without_any_property_values_leaves_properties_absent():
    # An attachment list with no PropertyValue entries -> no property at all.
    actor = {**ACTOR, "attachment": [{"type": "Image", "url": "https://x/y.png"}]}
    client = FakeActivityPubClient(actor=actor)
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 0))

    assert await graph.get_node_property(ACTOR_ID, "properties") is None


# ---------------------------------------------------------------------------
# outbox — the posts collection URL, stored verbatim. Not crawled here, but
# captured so a later content-indexing pass doesn't need to re-fetch the actor.
# ---------------------------------------------------------------------------

OUTBOX_URL = "https://cosocial.ca/users/evan/outbox"


async def test_stamps_outbox_url():
    actor = {**ACTOR, "outbox": OUTBOX_URL}
    client = FakeActivityPubClient(actor=actor)
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 0))

    assert await graph.get_node_property(ACTOR_ID, "outbox") == OUTBOX_URL


async def test_omitted_outbox_stays_absent():
    client = FakeActivityPubClient(actor=ACTOR)  # ACTOR carries no outbox
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 0))

    assert await graph.get_node_property(ACTOR_ID, "outbox") is None


# ---------------------------------------------------------------------------
# alsoKnownAs — account aliases (migration). Stored as a JSON-packed list of
# strings. AS2 serializes a single value as a bare string -> normalized up to a
# one-element list so the stored shape is always a list.
# ---------------------------------------------------------------------------


async def test_packs_also_known_as_list():
    aka = ["https://old.example/users/evan", "https://other.example/@evan"]
    actor = {**ACTOR, "alsoKnownAs": aka}
    client = FakeActivityPubClient(actor=actor)
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 0))

    stored = await graph.get_node_property(ACTOR_ID, "also_known_as")
    assert json.loads(stored) == aka


async def test_single_also_known_as_string_normalized_to_a_list():
    aka = "https://old.example/users/evan"
    actor = {**ACTOR, "alsoKnownAs": aka}
    client = FakeActivityPubClient(actor=actor)
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 0))

    stored = await graph.get_node_property(ACTOR_ID, "also_known_as")
    assert json.loads(stored) == [aka]


async def test_omitted_also_known_as_stays_absent():
    client = FakeActivityPubClient(actor=ACTOR)  # ACTOR carries no alsoKnownAs
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 0))

    assert await graph.get_node_property(ACTOR_ID, "also_known_as") is None


async def test_derives_hostname_from_the_actor_id():
    # Not carried on the actor doc — parsed out of the id URI's authority.
    client = FakeActivityPubClient()
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 0))

    # ACTOR_ID == "https://cosocial.ca/users/evan"
    assert await graph.get_node_property(ACTOR_ID, "hostname") == "cosocial.ca"


async def test_hostname_is_lowercased_without_port():
    # A noisy id: uppercase host + explicit port -> port-less, lowercased host,
    # so actors group cleanly by server (urlparse(...).hostname, not netloc).
    actor_id = "https://Example.COM:8443/users/bob"
    actor = {**ACTOR, "id": actor_id}
    client = FakeActivityPubClient(actor=actor)
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(actor_id, 0))

    assert await graph.get_node_property(actor_id, "hostname") == "example.com"


async def test_records_server_software_from_the_server_header():
    # Comes off the response's Server header, not the actor doc.
    client = FakeActivityPubClient(headers={"Server": SERVER})
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 0))

    assert await graph.get_node_property(ACTOR_ID, "server") == SERVER


async def test_absent_server_header_leaves_server_unset():
    # The Server header is frequently stripped -> no property, not a blank string.
    client = FakeActivityPubClient(headers={})  # no Server header
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 0))

    assert await graph.get_node_property(ACTOR_ID, "server") is None


async def test_enriches_an_existing_bare_node():
    client = FakeActivityPubClient()
    graph = FakeGraph()
    await graph.ensure_node(ACTOR_ID)  # bare node from WebfingerHandler / PageHandler

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 1))

    assert client.calls == [ACTOR_ID]
    assert await graph.get_node_property(ACTOR_ID, "type") == "Person"
    assert await graph.get_node_property(ACTOR_ID, "last_fetch_date") is not None


async def test_skips_an_already_fetched_node():
    client = FakeActivityPubClient()
    graph = FakeGraph()
    await graph.ensure_node(ACTOR_ID)
    await graph.set_node_property(ACTOR_ID, "type", "Person")
    await graph.set_node_property(ACTOR_ID, "last_fetch_date", "2026-06-01T00:00:00")
    dis = FakeDispatcher()

    await make_handler(client, graph, dis).handle(actor_job(ACTOR_ID, 0))

    # Already stamped -> no re-fetch, no enqueue.
    assert client.calls == []
    assert dis.enqueued == []


async def test_fetch_failure_propagates_and_leaves_node_unstamped():
    client = FakeActivityPubClient(error=RuntimeError("boom"))
    graph = FakeGraph()
    await graph.ensure_node(ACTOR_ID)
    dis = FakeDispatcher()

    with pytest.raises(RuntimeError):
        await make_handler(client, graph, dis).handle(actor_job(ACTOR_ID, 0))

    assert await graph.get_node_property(ACTOR_ID, "last_fetch_date") is None
    assert dis.enqueued == []


# ---------------------------------------------------------------------------
# HTTP status: record reachability (410 == permanently gone, derived downstream)
#   200            -> http_status 200
#   404/410/403/401 (typed HTTPStatusError) -> caught, recorded, NOT propagated
#   non-HTTP errors still propagate (see test_fetch_failure_propagates...)
# ---------------------------------------------------------------------------


async def test_records_http_status_200_on_success():
    client = FakeActivityPubClient()
    graph = FakeGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 0))

    assert await graph.get_node_property(ACTOR_ID, "http_status") == 200


@pytest.mark.parametrize("status", [404, 410, 403, 401])
async def test_records_http_status_for_error_responses(status):
    client = FakeActivityPubClient(error=http_status_error(status))
    graph = FakeGraph()
    dis = FakeDispatcher()

    # Caught and recorded, NOT propagated — no actor doc, so nothing enqueued.
    # (410 == gone is a downstream read of this code, not a separate property.)
    await make_handler(client, graph, dis).handle(actor_job(ACTOR_ID, 0))

    assert await graph.get_node_property(ACTOR_ID, "http_status") == status
    assert dis.enqueued == []


# ---------------------------------------------------------------------------
# Enqueue collections — unconditional (leaves get counts too)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("depth", [0, 5])
async def test_enqueues_both_collections_carrying_the_actor_depth(depth):
    client = FakeActivityPubClient()
    graph = FakeGraph()
    dis = FakeDispatcher()

    await make_handler(client, graph, dis).handle(actor_job(ACTOR_ID, depth))

    jobs = dis.enqueued
    assert len(jobs) == 2
    # No depth gate here — collections go out at any depth, carrying the actor's.
    assert collection_job(FOLLOWERS_URL, "followers", depth) in jobs
    assert collection_job(FOLLOWING_URL, "following", depth) in jobs


def test_next_available_delegates_to_the_client_for_the_actor_url():
    client = FakeActivityPubClient()
    handler = make_handler(client, FakeGraph(), FakeDispatcher())

    result = handler.next_available(actor_job(ACTOR_ID, 0))

    # It HANDLES actor jobs, so it asks its client about the actor URL it'll fetch.
    assert result == NA_RESULT
    assert client.na_calls == [ACTOR_ID]
