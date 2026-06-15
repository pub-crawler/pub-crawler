"""Tests for PageHandler — fan a collection page out into edges + actor/next jobs.

handle fetches the page and, for each member (from orderedItems or items):
adds a follow EDGE (direction sets orientation) and enqueues an actor job at
depth+1. If the page has a next, it enqueues a page job for it.

A member may be an id string or an embedded actor object; either way we use its
id. Members are added as BARE endpoint nodes — ActorHandler enriches them later.

Pure DI unit tests: a fake async client, a recording FakeDispatcher, a FakeGraph.

Assumed contract (flag if different):
  PageHandler(client, dispatcher, graph).handle(job)
    job = {job_type:'page', page_id, owner_id, direction, depth}
    page = await client.get(page_id)
    for member in page.orderedItems or page.items:
        m = member if str else member['id']
        followers: add edge m -> owner_id ; following: add edge owner_id -> m
        enqueue {job_type:'actor', actor_id:m, depth: job.depth + 1}
    if page.next:
        enqueue {job_type:'page', page_id:next, owner_id, direction, depth: job.depth}
"""

import httpx
import pytest

from pub_crawler.page_handler import PageHandler
from support import FakeDispatcher, FakeGraph

PAGE_ID = "https://example.com/foo/followers/1"
NEXT_ID = "https://example.com/foo/followers/2"
OWNER_ID = "https://example.com/foo"
DIRECTION = "followers"
DEPTH = 1
ITEM_A = "https://a.example/users/a"
ITEM_B = "https://b.example/users/b"


def input_job(direction=DIRECTION):
    return {
        "job_type": "page",
        "page_id": PAGE_ID,
        "owner_id": OWNER_ID,
        "direction": direction,
        "depth": DEPTH,
    }


def page_doc(items, next_id=None, items_key="orderedItems"):
    doc = {"id": PAGE_ID, "type": "OrderedCollectionPage", items_key: items}
    if next_id is not None:
        doc["next"] = next_id
    return doc


def actor_job(actor_id, depth):
    return {"job_type": "actor", "actor_id": actor_id, "depth": depth}


def next_page_job(next_id):
    return {
        "job_type": "page",
        "page_id": next_id,
        "owner_id": OWNER_ID,
        "direction": DIRECTION,
        "depth": DEPTH,
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


# ---------------------------------------------------------------------------
# Actor jobs (depth+1), from orderedItems / items
# ---------------------------------------------------------------------------


async def test_enqueues_an_actor_job_per_ordered_item():
    client = FakeActivityPubClient(doc=page_doc([ITEM_A, ITEM_B]))
    dis = FakeDispatcher()

    await PageHandler(client, dis, FakeGraph()).handle(input_job())

    actor_jobs = [j for j in dis.enqueued if j["job_type"] == "actor"]
    assert len(actor_jobs) == 2
    # Members are one hop further out than the page -> depth + 1.
    assert actor_job(ITEM_A, DEPTH + 1) in actor_jobs
    assert actor_job(ITEM_B, DEPTH + 1) in actor_jobs


async def test_handles_plain_items_key():
    client = FakeActivityPubClient(doc=page_doc([ITEM_A], items_key="items"))
    dis = FakeDispatcher()

    await PageHandler(client, dis, FakeGraph()).handle(input_job())

    actor_jobs = [j for j in dis.enqueued if j["job_type"] == "actor"]
    assert actor_jobs == [actor_job(ITEM_A, DEPTH + 1)]


async def test_handles_embedded_actor_objects():
    items = [{"id": ITEM_A, "type": "Person", "preferredUsername": "a"}]
    client = FakeActivityPubClient(doc=page_doc(items))
    dis = FakeDispatcher()
    graph = FakeGraph()

    await PageHandler(client, dis, graph).handle(input_job())

    actor_jobs = [j for j in dis.enqueued if j["job_type"] == "actor"]
    assert actor_jobs == [actor_job(ITEM_A, DEPTH + 1)]  # uses the embedded id
    assert await graph.has_edge(ITEM_A, OWNER_ID)  # ...and so does the edge


async def test_does_not_enqueue_an_actor_job_for_an_already_crawled_member():
    client = FakeActivityPubClient(doc=page_doc([ITEM_A]))
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(ITEM_A)
    await graph.set_node_property(ITEM_A, "last_fetch_date", "2026-06-01T00:00:00")

    await PageHandler(client, dis, graph).handle(input_job())

    # Already fetched -> no redundant actor job...
    assert [j for j in dis.enqueued if j["job_type"] == "actor"] == []
    # ...but the follow edge is still recorded (it's valid regardless).
    assert await graph.has_edge(ITEM_A, OWNER_ID)


# ---------------------------------------------------------------------------
# Edges (direction sets orientation)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "direction, edge",
    [
        ("followers", (ITEM_A, OWNER_ID)),  # a follower follows the owner
        ("following", (OWNER_ID, ITEM_A)),  # the owner follows a followee
    ],
)
async def test_adds_a_follow_edge_per_member(direction, edge):
    client = FakeActivityPubClient(doc=page_doc([ITEM_A]))
    graph = FakeGraph()

    await PageHandler(client, FakeDispatcher(), graph).handle(input_job(direction))

    assert await graph.has_edge(*edge)
    assert len([e async for e in graph.all_edges()]) == 1
    # The member is a BARE endpoint node — ActorHandler enriches it later.
    assert await graph.get_node_properties(ITEM_A) == {}


@pytest.mark.parametrize(
    "direction, edge",
    [
        ("followers", (ITEM_A, OWNER_ID)),
        ("following", (OWNER_ID, ITEM_A)),
    ],
)
async def test_stamps_the_from_direction_flag_on_each_edge(direction, edge):
    client = FakeActivityPubClient(doc=page_doc([ITEM_A]))
    graph = FakeGraph()

    await PageHandler(client, FakeDispatcher(), graph).handle(input_job(direction))

    # The follow edge carries the from_{direction} flag.
    assert await graph.get_edge_property(*edge, f"from_{direction}") is True


async def test_skips_members_with_a_falsy_id():
    # Empty/None ids are skipped; valid siblings are still wired up and enqueued.
    client = FakeActivityPubClient(doc=page_doc(["", ITEM_A, None]))
    dis = FakeDispatcher()
    graph = FakeGraph()

    await PageHandler(client, dis, graph).handle(input_job())

    assert await graph.has_edge(ITEM_A, OWNER_ID)
    assert [j for j in dis.enqueued if j["job_type"] == "actor"] == [
        actor_job(ITEM_A, DEPTH + 1)
    ]
    # The falsy ids produced no node, edge, or actor job.
    assert not await graph.has_node("")
    assert not await graph.has_node(None)


# ---------------------------------------------------------------------------
# next -> page job
# ---------------------------------------------------------------------------


async def test_enqueues_next_as_a_page_job():
    client = FakeActivityPubClient(doc=page_doc([ITEM_A], next_id=NEXT_ID))
    dis = FakeDispatcher()

    await PageHandler(client, dis, FakeGraph()).handle(input_job())

    page_jobs = [j for j in dis.enqueued if j["job_type"] == "page"]
    # Same owner/direction/depth — it's more of the same collection.
    assert page_jobs == [next_page_job(NEXT_ID)]


async def test_no_next_means_no_page_job():
    client = FakeActivityPubClient(doc=page_doc([ITEM_A]))  # no next
    dis = FakeDispatcher()

    await PageHandler(client, dis, FakeGraph()).handle(input_job())

    page_jobs = [j for j in dis.enqueued if j["job_type"] == "page"]
    assert page_jobs == []


async def test_filtered_page_with_next_but_no_items_still_pages_on():
    # An AP server filtering its member list can return a page with `next` but no
    # items. We still record the page and walk to `next`; there's just no member
    # work to do.
    client = FakeActivityPubClient(doc=page_doc([], next_id=NEXT_ID))
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)

    await PageHandler(client, dis, graph).handle(input_job())

    # Owner-side recorded and the next page enqueued...
    assert await graph.get_node_property(OWNER_ID, f"{DIRECTION}_last_page") == PAGE_ID
    assert (
        await graph.get_node_property(OWNER_ID, f"{DIRECTION}_last_page_http_status")
        == 200
    )
    assert [j for j in dis.enqueued if j["job_type"] == "page"] == [
        next_page_job(NEXT_ID)
    ]
    # ...but no member work.
    assert [j for j in dis.enqueued if j["job_type"] == "actor"] == []
    assert [e async for e in graph.all_edges()] == []


# ---------------------------------------------------------------------------
# Page-walk progress recorded on the owner node, keyed by direction
#   {direction}_last_page             -> the page just visited
#   {direction}_last_page_http_status -> its fetch status (200, or error code)
#   {direction}_pages_complete        -> True only when the last visited page was
#                                        a successful terminal page (no `next`)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("direction", ["followers", "following"])
async def test_records_last_page_and_marks_complete_on_a_terminal_page(direction):
    client = FakeActivityPubClient(doc=page_doc([ITEM_A]))  # no next
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)

    await PageHandler(client, FakeDispatcher(), graph).handle(input_job(direction))

    assert await graph.get_node_property(OWNER_ID, f"{direction}_last_page") == PAGE_ID
    assert (
        await graph.get_node_property(OWNER_ID, f"{direction}_last_page_http_status")
        == 200
    )
    # No `next` -> we reached the end.
    assert (
        await graph.get_node_property(OWNER_ID, f"{direction}_pages_complete") is True
    )


@pytest.mark.parametrize("direction", ["followers", "following"])
async def test_pages_incomplete_while_a_next_remains(direction):
    client = FakeActivityPubClient(doc=page_doc([ITEM_A], next_id=NEXT_ID))
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)

    await PageHandler(client, FakeDispatcher(), graph).handle(input_job(direction))

    # Visited this page fine, but there's more to come -> not complete yet.
    assert await graph.get_node_property(OWNER_ID, f"{direction}_last_page") == PAGE_ID
    assert (
        await graph.get_node_property(OWNER_ID, f"{direction}_pages_complete") is False
    )


@pytest.mark.parametrize("status", [404, 410, 403, 401])
async def test_records_status_and_marks_incomplete_on_a_page_error(status):
    client = FakeActivityPubClient(error=http_status_error(status, PAGE_ID))
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)

    # Caught and recorded, NOT propagated — a failed fetch never reaches the end.
    await PageHandler(client, dis, graph).handle(input_job())

    assert await graph.get_node_property(OWNER_ID, f"{DIRECTION}_last_page") == PAGE_ID
    assert (
        await graph.get_node_property(OWNER_ID, f"{DIRECTION}_last_page_http_status")
        == status
    )
    assert (
        await graph.get_node_property(OWNER_ID, f"{DIRECTION}_pages_complete") is False
    )
    assert dis.enqueued == []


async def test_non_http_failure_propagates_but_records_the_attempt():
    # A non-HTTPStatusError (timeout, connection reset, ...) is NOT caught, so it
    # propagates. But the page is marked before the fetch, so the attempt is still
    # recorded: last_page set, pages_complete left False (never reached the end).
    client = FakeActivityPubClient(error=RuntimeError("boom"))
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(OWNER_ID)

    with pytest.raises(RuntimeError):
        await PageHandler(client, dis, graph).handle(input_job())

    assert await graph.get_node_property(OWNER_ID, f"{DIRECTION}_last_page") == PAGE_ID
    assert (
        await graph.get_node_property(OWNER_ID, f"{DIRECTION}_pages_complete") is False
    )
    # No status recorded (no response), and nothing enqueued.
    assert (
        await graph.get_node_property(OWNER_ID, f"{DIRECTION}_last_page_http_status")
        is None
    )
    assert dis.enqueued == []


def test_next_available_delegates_to_the_client_for_the_page_url():
    client = FakeActivityPubClient()
    handler = PageHandler(client, FakeDispatcher(), FakeGraph())

    result = handler.next_available(input_job())

    # It HANDLES page jobs, so it asks its client about the page URL it'll fetch.
    assert result == NA_RESULT
    assert client.na_calls == [PAGE_ID]
