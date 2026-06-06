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

Pure DI unit tests: a fake async client, a real asyncio.Queue, a real DiGraph.

Assumed contract (flag if different):
  CollectionHandler(client, queue, graph, max_depth).handle(job)
    job = {job_type:'collection', collection_id, owner_id, direction, depth}
    coll = await client.get(collection_id)
    graph.nodes[owner_id][f"{direction}_count"] = coll["totalItems"]
    if depth < max_depth:
      if coll has 'first':
        enqueue {job_type:'page', page_id:first, owner_id, direction, depth}
      elif coll has inline items (orderedItems/items):
        for each member (string id or dict->id):
          add node, add edge (followers: member->owner, following: owner->member),
          enqueue {job_type:'actor', actor_id, depth+1}
"""

import asyncio

import networkx as nx
import pytest

from pub_crawler.collection_handler import CollectionHandler
from support import FakeDispatcher

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


def drain(queue):
    jobs = []
    while not queue.empty():
        jobs.append(queue.get_nowait())
    return jobs


NA_RESULT = 4242


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


def make_handler(client, queue, graph, max_depth=MAX_DEPTH):
    return CollectionHandler(client, FakeDispatcher(queue), graph, max_depth)


# ---------------------------------------------------------------------------
# Count (unconditional)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "direction, collection_id",
    [("followers", FOLLOWERS_ID), ("following", FOLLOWING_ID)],
)
async def test_saves_count_on_owner_node_keyed_by_direction(direction, collection_id):
    client = FakeActivityPubClient(doc=collection(collection_id))
    queue = asyncio.Queue()
    graph = nx.DiGraph()
    graph.add_node(OWNER_ID)

    await make_handler(client, queue, graph).handle(
        collection_job(collection_id, direction, 0)
    )

    assert client.calls == [collection_id]
    assert graph.nodes[OWNER_ID][f"{direction}_count"] == TOTAL


async def test_count_does_not_clobber_owner_metadata():
    client = FakeActivityPubClient(doc=collection(FOLLOWERS_ID))
    queue = asyncio.Queue()
    graph = nx.DiGraph()
    graph.add_node(OWNER_ID, type="Person", followers=FOLLOWERS_ID)

    await make_handler(client, queue, graph).handle(
        collection_job(FOLLOWERS_ID, "followers", 0)
    )

    node = graph.nodes[OWNER_ID]
    assert node["followers_count"] == TOTAL  # count under a distinct key...
    assert node["followers"] == FOLLOWERS_ID  # ...so the URL attribute survives
    assert node["type"] == "Person"


async def test_fetch_failure_propagates():
    client = FakeActivityPubClient(error=RuntimeError("boom"))
    queue = asyncio.Queue()
    graph = nx.DiGraph()
    graph.add_node(OWNER_ID)

    with pytest.raises(RuntimeError):
        await make_handler(client, queue, graph).handle(
            collection_job(FOLLOWERS_ID, "followers", 0)
        )

    assert queue.empty()


# ---------------------------------------------------------------------------
# Enqueue the first page, gated by depth < max_depth
# ---------------------------------------------------------------------------


async def test_enqueues_the_first_page_below_max_depth():
    client = FakeActivityPubClient(doc=collection(FOLLOWERS_ID))
    queue = asyncio.Queue()
    graph = nx.DiGraph()
    graph.add_node(OWNER_ID)

    # depth 0 < MAX_DEPTH
    await make_handler(client, queue, graph).handle(
        collection_job(FOLLOWERS_ID, "followers", 0)
    )

    page_jobs = [j for j in drain(queue) if j["job_type"] == "page"]
    # First page carries the collection's owner/direction/depth.
    assert page_jobs == [page_job(f"{FOLLOWERS_ID}?page=1", "followers", 0)]


async def test_does_not_enqueue_the_page_at_max_depth():
    client = FakeActivityPubClient(doc=collection(FOLLOWERS_ID))
    queue = asyncio.Queue()
    graph = nx.DiGraph()
    graph.add_node(OWNER_ID)

    await make_handler(client, queue, graph).handle(
        collection_job(FOLLOWERS_ID, "followers", MAX_DEPTH)
    )

    # Leaf collection: counted, but not walked.
    assert graph.nodes[OWNER_ID]["followers_count"] == TOTAL
    assert queue.empty()


# ---------------------------------------------------------------------------
# Inline / unpaged collections: members on the collection, no `first`
# ---------------------------------------------------------------------------


async def test_walks_inline_ordered_items_below_max_depth():
    members = [MEMBER_A, MEMBER_B]
    client = FakeActivityPubClient(doc=inline_collection(FOLLOWERS_ID, members))
    queue = asyncio.Queue()
    graph = nx.DiGraph()
    graph.add_node(OWNER_ID)

    await make_handler(client, queue, graph).handle(
        collection_job(FOLLOWERS_ID, "followers", 0)
    )

    # Walked straight from the doc we already fetched — no re-fetch / page job.
    assert client.calls == [FOLLOWERS_ID]
    assert graph.nodes[OWNER_ID]["followers_count"] == 2
    # Each member: a node and a followers edge (member -> owner).
    for m in members:
        assert m in graph.nodes
        assert graph.has_edge(m, OWNER_ID)
    # And an actor job at depth+1 for each, in order — no page jobs.
    jobs = drain(queue)
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
    queue = asyncio.Queue()
    graph = nx.DiGraph()
    graph.add_node(OWNER_ID)

    await make_handler(client, queue, graph).handle(
        collection_job(FOLLOWERS_ID, "followers", 0)
    )

    assert graph.has_edge(MEMBER_A, OWNER_ID)
    assert [j for j in drain(queue) if j["job_type"] == "actor"] == [
        actor_job(MEMBER_A, 1)
    ]


async def test_inline_following_direction_orients_edges_from_owner():
    client = FakeActivityPubClient(doc=inline_collection(FOLLOWING_ID, [MEMBER_A]))
    queue = asyncio.Queue()
    graph = nx.DiGraph()
    graph.add_node(OWNER_ID)

    await make_handler(client, queue, graph).handle(
        collection_job(FOLLOWING_ID, "following", 0)
    )

    # following: owner -> member (the mirror of followers).
    assert graph.has_edge(OWNER_ID, MEMBER_A)
    assert not graph.has_edge(MEMBER_A, OWNER_ID)


async def test_inline_member_dicts_use_their_id():
    members = [{"id": MEMBER_A, "type": "Person"}, {"id": MEMBER_B, "type": "Person"}]
    client = FakeActivityPubClient(doc=inline_collection(FOLLOWERS_ID, members))
    queue = asyncio.Queue()
    graph = nx.DiGraph()
    graph.add_node(OWNER_ID)

    await make_handler(client, queue, graph).handle(
        collection_job(FOLLOWERS_ID, "followers", 0)
    )

    assert graph.has_edge(MEMBER_A, OWNER_ID)
    assert graph.has_edge(MEMBER_B, OWNER_ID)


async def test_does_not_walk_inline_items_at_max_depth():
    client = FakeActivityPubClient(
        doc=inline_collection(FOLLOWERS_ID, [MEMBER_A, MEMBER_B])
    )
    queue = asyncio.Queue()
    graph = nx.DiGraph()
    graph.add_node(OWNER_ID)

    await make_handler(client, queue, graph).handle(
        collection_job(FOLLOWERS_ID, "followers", MAX_DEPTH)
    )

    # Leaf: counted, but members NOT walked — same rule as the paged leaf, and it
    # prevents enqueuing actors at max_depth+1 (depth overrun).
    assert graph.nodes[OWNER_ID]["followers_count"] == 2
    assert graph.number_of_edges() == 0
    assert queue.empty()


async def test_prefers_pagination_when_both_first_and_inline_present():
    # Rare, but some collections show an inline preview AND offer a `first` page.
    doc = inline_collection(FOLLOWERS_ID, [MEMBER_A])
    doc["first"] = f"{FOLLOWERS_ID}?page=1"
    client = FakeActivityPubClient(doc=doc)
    queue = asyncio.Queue()
    graph = nx.DiGraph()
    graph.add_node(OWNER_ID)

    await make_handler(client, queue, graph).handle(
        collection_job(FOLLOWERS_ID, "followers", 0)
    )

    # Paginate; don't ALSO process the inline preview (would double-count members).
    jobs = drain(queue)
    assert [j for j in jobs if j["job_type"] == "page"] == [
        page_job(f"{FOLLOWERS_ID}?page=1", "followers", 0)
    ]
    assert [j for j in jobs if j["job_type"] == "actor"] == []
    assert not graph.has_edge(MEMBER_A, OWNER_ID)


def test_next_available_delegates_to_the_client_for_the_collection_url():
    client = FakeActivityPubClient()
    handler = make_handler(client, asyncio.Queue(), nx.DiGraph())

    result = handler.next_available(collection_job(FOLLOWERS_ID, "followers", 0))

    # It HANDLES collection jobs, so it asks its client about the collection URL.
    assert result == NA_RESULT
    assert client.na_calls == [FOLLOWERS_ID]
