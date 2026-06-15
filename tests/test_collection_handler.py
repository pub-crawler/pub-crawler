"""Tests for CollectionHandler — save the count, and (now) enqueue the first page.

handle fetches the collection (followers/following), stores its totalItems on
the owner's node keyed by direction (followers_count / following_count), and —
now — enqueues a page job for the collection's `first`, gated by
depth < max_depth (so a leaf collection at depth == max_depth gets counted but
not walked).

Two collection shapes are handled:
  - PAGINATED: a `first` link -> enqueue a page job (PageHandler walks it).
  - INLINE / UNPAGED: orderedItems/items carried ON the collection itself, with
    no `first` (NodeBB / activitypub.space style) -> walk those members directly
    from the doc we ALREADY fetched (no re-fetch / no page round-trip), mirroring
    PageHandler's per-member logic. Both member-walks are gated by
    depth < max_depth, so a leaf collection is counted but not expanded.

Pure DI unit tests: a fake async client, a recording FakeDispatcher, a FakeGraph.

Assumed contract (flag if different):
  CollectionHandler(client, dispatcher, graph, max_depth).handle(job)
    job = {job_type:'collection', collection_id, owner_id, direction, depth}
    coll = await client.get(collection_id)
    await graph.set_node_property(owner_id, f"{direction}_count", coll["totalItems"])
    if depth < max_depth:
      if coll has 'first':
        enqueue {job_type:'page', page_id:first, owner_id, direction, depth}
      elif coll has inline items (orderedItems/items):
        for each member (string id or dict->id):
          ensure node, ensure edge (followers: member->owner, following: owner->member),
          enqueue {job_type:'actor', actor_id, depth+1}
"""

import httpx
import pytest

from pub_crawler.collection_handler import CollectionHandler
from support import FakeDispatcher, FakeGraph

OWNER_ID = "https://example.com/foo"
FOLLOWERS_ID = "https://example.com/foo/followers"
FOLLOWING_ID = "https://example.com/foo/following"
MEMBER_A = "https://a.example/users/alice"
MEMBER_B = "https://b.example/users/bob"
TOTAL = 42
MAX_DEPTH = 2


def collection(collection_id, total=TOTAL):
    return {
        "id": collection_id,
        "type": "OrderedCollection",
        "totalItems": total,
        "first": f"{collection_id}?page=1",
    }


def collection_job(collection_id, direction, depth):
    return {
        "job_type": "collection",
        "collection_id": collection_id,
        "owner_id": OWNER_ID,
        "direction": direction,
        "depth": depth,
    }


def page_job(page_id, direction, depth):
    return {
        "job_type": "page",
        "page_id": page_id,
        "owner_id": OWNER_ID,
        "direction": direction,
        "depth": depth,
    }


def inline_collection(collection_id, members, total=None, key="orderedItems"):
    """A collection that carries its members inline, with NO `first` link."""
    return {
        "id": collection_id,
        "type": "OrderedCollection",
        "totalItems": len(members) if total is None else total,
        key: members,
    }


def actor_job(actor_id, depth):
    # Mirrors exactly what PageHandler enqueues for each member.
    return {"job_type": "actor", "actor_id": actor_id, "depth": depth}


def hidden_collection(collection_id, total=TOTAL):
    """A collection that exposes only its count: no `first`, no inline members.

    The common locked-account shape — the actor advertises totalItems but
    refuses to enumerate who's in the list.
    """
    return {
        "id": collection_id,
        "type": "OrderedCollection",
        "totalItems": total,
    }


NA_RESULT = 4242


def http_status_error(status, url):
    """The typed error httpx raises from response.raise_for_status() — carries
    the code at .response.status_code."""
    request = httpx.Request("GET", url)
    return httpx.HTTPStatusError(
        f"{status}", request=request, response=httpx.Response(status, request=request)
    )


class FakeActivityPubClient:
    def __init__(self, doc=None, error=None):
        self.doc = doc
        self.error = error
        self.calls = []
        self.na_calls = []

    async def get(self, url):
        self.calls.append(url)
        if self.error is not None:
            raise self.error
        return self.doc

    def next_available(self, url):
        self.na_calls.append(url)
        return NA_RESULT


def make_handler(client, dispatcher, graph, max_depth=MAX_DEPTH):
    return CollectionHandler(client, dispatcher, graph, max_depth)


# ---------------------------------------------------------------------------
# Count (unconditional)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "direction, collection_id",
    [("followers", FOLLOWERS_ID), ("following", FOLLOWING_ID)],
)
async def test_saves_count_on_owner_node_keyed_by_direction(direction, collection_id):
    client = FakeActivityPubClient(doc=collection(collection_id))
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)

    await make_handler(client, dis, graph).handle(
        collection_job(collection_id, direction, 0)
    )

    assert client.calls == [collection_id]
    assert await graph.get_node_property(OWNER_ID, f"{direction}_count") == TOTAL


async def test_count_does_not_clobber_owner_metadata():
    client = FakeActivityPubClient(doc=collection(FOLLOWERS_ID))
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)
    await graph.set_node_property(OWNER_ID, "type", "Person")
    await graph.set_node_property(OWNER_ID, "followers", FOLLOWERS_ID)

    await make_handler(client, dis, graph).handle(
        collection_job(FOLLOWERS_ID, "followers", 0)
    )

    assert await graph.get_node_property(OWNER_ID, "followers_count") == TOTAL  # count under a distinct key...
    assert await graph.get_node_property(OWNER_ID, "followers") == FOLLOWERS_ID  # ...URL survives
    assert await graph.get_node_property(OWNER_ID, "type") == "Person"


async def test_records_a_zero_count():
    # A genuinely empty collection (totalItems: 0) must still record the count —
    # 0 is a real value, not "unknown". (Pins the fix for the truthy-drop bug.)
    client = FakeActivityPubClient(doc=hidden_collection(FOLLOWERS_ID, total=0))
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)

    await make_handler(client, dis, graph).handle(
        collection_job(FOLLOWERS_ID, "followers", 0)
    )

    assert await graph.get_node_property(OWNER_ID, "followers_count") == 0


async def test_omitted_total_items_leaves_count_absent():
    # No totalItems advertised -> count stays unset (unknown, distinct from 0).
    doc = {"id": FOLLOWERS_ID, "type": "OrderedCollection"}  # no totalItems
    client = FakeActivityPubClient(doc=doc)
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)

    await make_handler(client, dis, graph).handle(
        collection_job(FOLLOWERS_ID, "followers", 0)
    )

    assert await graph.get_node_property(OWNER_ID, "followers_count") is None


async def test_fetch_failure_propagates():
    client = FakeActivityPubClient(error=RuntimeError("boom"))
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)

    with pytest.raises(RuntimeError):
        await make_handler(client, dis, graph).handle(
            collection_job(FOLLOWERS_ID, "followers", 0)
        )

    assert dis.enqueued == []


# ---------------------------------------------------------------------------
# Enqueue the first page, gated by depth < max_depth
# ---------------------------------------------------------------------------


async def test_enqueues_the_first_page_below_max_depth():
    client = FakeActivityPubClient(doc=collection(FOLLOWERS_ID))
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)

    # depth 0 < MAX_DEPTH
    await make_handler(client, dis, graph).handle(
        collection_job(FOLLOWERS_ID, "followers", 0)
    )

    page_jobs = [j for j in dis.enqueued if j["job_type"] == "page"]
    # First page carries the collection's owner/direction/depth.
    assert page_jobs == [page_job(f"{FOLLOWERS_ID}?page=1", "followers", 0)]


async def test_does_not_enqueue_the_page_at_max_depth():
    client = FakeActivityPubClient(doc=collection(FOLLOWERS_ID))
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)

    await make_handler(client, dis, graph).handle(
        collection_job(FOLLOWERS_ID, "followers", MAX_DEPTH)
    )

    # Leaf collection: counted, but not walked.
    assert await graph.get_node_property(OWNER_ID, "followers_count") == TOTAL
    assert dis.enqueued == []


# ---------------------------------------------------------------------------
# Inline / unpaged collections: members on the collection, no `first`
# ---------------------------------------------------------------------------


async def test_walks_inline_ordered_items_below_max_depth():
    members = [MEMBER_A, MEMBER_B]
    client = FakeActivityPubClient(doc=inline_collection(FOLLOWERS_ID, members))
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)

    await make_handler(client, dis, graph).handle(
        collection_job(FOLLOWERS_ID, "followers", 0)
    )

    # Walked straight from the doc we already fetched — no re-fetch / page job.
    assert client.calls == [FOLLOWERS_ID]
    assert await graph.get_node_property(OWNER_ID, "followers_count") == 2
    # Each member: a node and a followers edge (member -> owner).
    for m in members:
        assert await graph.has_node(m)
        assert await graph.has_edge(m, OWNER_ID)
    # And an actor job at depth+1 for each, in order — no page jobs.
    jobs = dis.enqueued
    assert [j for j in jobs if j["job_type"] == "page"] == []
    assert [j for j in jobs if j["job_type"] == "actor"] == [
        actor_job(MEMBER_A, 1),
        actor_job(MEMBER_B, 1),
    ]


async def test_walks_inline_items_key():
    # A non-ordered Collection uses `items`, not `orderedItems`.
    client = FakeActivityPubClient(
        doc=inline_collection(FOLLOWERS_ID, [MEMBER_A], key="items")
    )
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)

    await make_handler(client, dis, graph).handle(
        collection_job(FOLLOWERS_ID, "followers", 0)
    )

    assert await graph.has_edge(MEMBER_A, OWNER_ID)
    assert [j for j in dis.enqueued if j["job_type"] == "actor"] == [
        actor_job(MEMBER_A, 1)
    ]


async def test_does_not_enqueue_an_actor_job_for_an_already_crawled_member():
    client = FakeActivityPubClient(doc=inline_collection(FOLLOWERS_ID, [MEMBER_A]))
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)
    await graph.ensure_node(MEMBER_A)
    await graph.set_node_property(MEMBER_A, "last_fetch_date", "2026-06-01T00:00:00")

    await make_handler(client, dis, graph).handle(
        collection_job(FOLLOWERS_ID, "followers", 0)
    )

    # Already fetched -> no redundant actor job, but the edge still lands.
    assert [j for j in dis.enqueued if j["job_type"] == "actor"] == []
    assert await graph.has_edge(MEMBER_A, OWNER_ID)


async def test_inline_following_direction_orients_edges_from_owner():
    client = FakeActivityPubClient(doc=inline_collection(FOLLOWING_ID, [MEMBER_A]))
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)

    await make_handler(client, dis, graph).handle(
        collection_job(FOLLOWING_ID, "following", 0)
    )

    # following: owner -> member (the mirror of followers).
    assert await graph.has_edge(OWNER_ID, MEMBER_A)
    assert not await graph.has_edge(MEMBER_A, OWNER_ID)


async def test_inline_member_dicts_use_their_id():
    members = [{"id": MEMBER_A, "type": "Person"}, {"id": MEMBER_B, "type": "Person"}]
    client = FakeActivityPubClient(doc=inline_collection(FOLLOWERS_ID, members))
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)

    await make_handler(client, dis, graph).handle(
        collection_job(FOLLOWERS_ID, "followers", 0)
    )

    assert await graph.has_edge(MEMBER_A, OWNER_ID)
    assert await graph.has_edge(MEMBER_B, OWNER_ID)


async def test_inline_followers_stamps_the_from_followers_edge_flag():
    client = FakeActivityPubClient(doc=inline_collection(FOLLOWERS_ID, [MEMBER_A]))
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)

    await make_handler(client, dis, graph).handle(
        collection_job(FOLLOWERS_ID, "followers", 0)
    )

    # The member -> owner edge carries the from_followers flag.
    assert await graph.get_edge_property(MEMBER_A, OWNER_ID, "from_followers") is True


async def test_inline_following_stamps_the_from_following_edge_flag():
    client = FakeActivityPubClient(doc=inline_collection(FOLLOWING_ID, [MEMBER_A]))
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)

    await make_handler(client, dis, graph).handle(
        collection_job(FOLLOWING_ID, "following", 0)
    )

    # The owner -> member edge carries the from_following flag.
    assert await graph.get_edge_property(OWNER_ID, MEMBER_A, "from_following") is True


async def test_inline_skips_members_with_a_falsy_id():
    # Empty/None ids are skipped; valid siblings are still wired up and enqueued.
    members = ["", MEMBER_A, None]
    client = FakeActivityPubClient(doc=inline_collection(FOLLOWERS_ID, members))
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)

    await make_handler(client, dis, graph).handle(
        collection_job(FOLLOWERS_ID, "followers", 0)
    )

    assert await graph.has_edge(MEMBER_A, OWNER_ID)
    assert [j for j in dis.enqueued if j["job_type"] == "actor"] == [
        actor_job(MEMBER_A, 1)
    ]
    # The falsy ids produced no node, edge, or actor job.
    assert not await graph.has_node("")
    assert not await graph.has_node(None)


async def test_does_not_walk_inline_items_at_max_depth():
    client = FakeActivityPubClient(
        doc=inline_collection(FOLLOWERS_ID, [MEMBER_A, MEMBER_B])
    )
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)

    await make_handler(client, dis, graph).handle(
        collection_job(FOLLOWERS_ID, "followers", MAX_DEPTH)
    )

    # Leaf: counted, but members NOT walked — same rule as the paged leaf, and it
    # prevents enqueuing actors at max_depth+1 (depth overrun).
    assert await graph.get_node_property(OWNER_ID, "followers_count") == 2
    assert [e async for e in graph.all_edges()] == []
    assert dis.enqueued == []


async def test_prefers_pagination_when_both_first_and_inline_present():
    # Rare, but some collections show an inline preview AND offer a `first` page.
    doc = inline_collection(FOLLOWERS_ID, [MEMBER_A])
    doc["first"] = f"{FOLLOWERS_ID}?page=1"
    client = FakeActivityPubClient(doc=doc)
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)

    await make_handler(client, dis, graph).handle(
        collection_job(FOLLOWERS_ID, "followers", 0)
    )

    # Paginate; don't ALSO process the inline preview (would double-count members).
    jobs = dis.enqueued
    assert [j for j in jobs if j["job_type"] == "page"] == [
        page_job(f"{FOLLOWERS_ID}?page=1", "followers", 0)
    ]
    assert [j for j in jobs if j["job_type"] == "actor"] == []
    assert not await graph.has_edge(MEMBER_A, OWNER_ID)


# ---------------------------------------------------------------------------
# members_shared flag: did the actor actually expose its membership?
#   paged (`first`) or inline (items/orderedItems) -> True
#   neither (count only / locked account)          -> False
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "direction, collection_id",
    [("followers", FOLLOWERS_ID), ("following", FOLLOWING_ID)],
)
async def test_paged_collection_marks_members_shared(direction, collection_id):
    client = FakeActivityPubClient(doc=collection(collection_id))
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)

    await make_handler(client, dis, graph).handle(
        collection_job(collection_id, direction, 0)
    )

    assert await graph.get_node_property(OWNER_ID, f"{direction}_members_shared") is True


@pytest.mark.parametrize(
    "direction, collection_id",
    [("followers", FOLLOWERS_ID), ("following", FOLLOWING_ID)],
)
async def test_inline_collection_marks_members_shared(direction, collection_id):
    client = FakeActivityPubClient(doc=inline_collection(collection_id, [MEMBER_A]))
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)

    await make_handler(client, dis, graph).handle(
        collection_job(collection_id, direction, 0)
    )

    assert await graph.get_node_property(OWNER_ID, f"{direction}_members_shared") is True


@pytest.mark.parametrize(
    "direction, collection_id",
    [("followers", FOLLOWERS_ID), ("following", FOLLOWING_ID)],
)
async def test_collection_without_members_marks_members_not_shared(
    direction, collection_id
):
    client = FakeActivityPubClient(doc=hidden_collection(collection_id))
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)

    await make_handler(client, dis, graph).handle(
        collection_job(collection_id, direction, 0)
    )

    # No first / items / orderedItems -> the actor didn't share its membership.
    assert (
        await graph.get_node_property(OWNER_ID, f"{direction}_members_shared") is False
    )
    # The count is still recorded — hidden-but-counted is a real state — and with
    # nothing to walk, nothing is enqueued.
    assert await graph.get_node_property(OWNER_ID, f"{direction}_count") == TOTAL
    assert dis.enqueued == []


# ---------------------------------------------------------------------------
# HTTP status of the collection fetch, keyed by direction
#   200            -> {direction}_http_status 200
#   404/410/403/401 (typed HTTPStatusError) -> caught, recorded, NOT propagated
#   non-HTTP errors still propagate (see test_fetch_failure_propagates)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "direction, collection_id",
    [("followers", FOLLOWERS_ID), ("following", FOLLOWING_ID)],
)
async def test_records_http_status_200_on_success(direction, collection_id):
    client = FakeActivityPubClient(doc=collection(collection_id))
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)

    await make_handler(client, dis, graph).handle(
        collection_job(collection_id, direction, 0)
    )

    assert await graph.get_node_property(OWNER_ID, f"{direction}_http_status") == 200


@pytest.mark.parametrize(
    "direction, collection_id",
    [("followers", FOLLOWERS_ID), ("following", FOLLOWING_ID)],
)
@pytest.mark.parametrize("status", [404, 410, 403, 401])
async def test_records_http_status_for_error_responses(status, direction, collection_id):
    client = FakeActivityPubClient(error=http_status_error(status, collection_id))
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)

    # Caught and recorded, NOT propagated — no doc, so no count and no enqueue.
    await make_handler(client, dis, graph).handle(
        collection_job(collection_id, direction, 0)
    )

    assert await graph.get_node_property(OWNER_ID, f"{direction}_http_status") == status
    assert await graph.get_node_property(OWNER_ID, f"{direction}_count") is None
    assert dis.enqueued == []


def test_next_available_delegates_to_the_client_for_the_collection_url():
    client = FakeActivityPubClient()
    handler = make_handler(client, FakeDispatcher(), FakeGraph())

    result = handler.next_available(collection_job(FOLLOWERS_ID, "followers", 0))

    # It HANDLES collection jobs, so it asks its client about the collection URL.
    assert result == NA_RESULT
    assert client.na_calls == [FOLLOWERS_ID]
