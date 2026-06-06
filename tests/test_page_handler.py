"""Tests for PageHandler — fan a collection page out into edges + actor/next jobs.

handle fetches the page and, for each member (from orderedItems or items):
adds a follow EDGE (direction sets orientation) and enqueues an actor job at
depth+1. If the page has a next, it enqueues a page job for it.

A member may be an id string or an embedded actor object; either way we use its
id. Members are added as BARE endpoint nodes — ActorHandler enriches them later.

Pure DI unit tests: a fake async client, a real asyncio.Queue, a real DiGraph.

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

import asyncio

import networkx as nx
import pytest

from pub_crawler.page_handler import PageHandler
from support import FakeDispatcher

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


# ---------------------------------------------------------------------------
# Actor jobs (depth+1), from orderedItems / items
# ---------------------------------------------------------------------------


async def test_enqueues_an_actor_job_per_ordered_item():
    client = FakeActivityPubClient(doc=page_doc([ITEM_A, ITEM_B]))
    queue = asyncio.Queue()

    await PageHandler(client, FakeDispatcher(queue), nx.DiGraph()).handle(input_job())

    actor_jobs = [j for j in drain(queue) if j["job_type"] == "actor"]
    assert len(actor_jobs) == 2
    # Members are one hop further out than the page -> depth + 1.
    assert actor_job(ITEM_A, DEPTH + 1) in actor_jobs
    assert actor_job(ITEM_B, DEPTH + 1) in actor_jobs


async def test_handles_plain_items_key():
    client = FakeActivityPubClient(doc=page_doc([ITEM_A], items_key="items"))
    queue = asyncio.Queue()

    await PageHandler(client, FakeDispatcher(queue), nx.DiGraph()).handle(input_job())

    actor_jobs = [j for j in drain(queue) if j["job_type"] == "actor"]
    assert actor_jobs == [actor_job(ITEM_A, DEPTH + 1)]


async def test_handles_embedded_actor_objects():
    items = [{"id": ITEM_A, "type": "Person", "preferredUsername": "a"}]
    client = FakeActivityPubClient(doc=page_doc(items))
    queue = asyncio.Queue()
    graph = nx.DiGraph()

    await PageHandler(client, FakeDispatcher(queue), graph).handle(input_job())

    actor_jobs = [j for j in drain(queue) if j["job_type"] == "actor"]
    assert actor_jobs == [actor_job(ITEM_A, DEPTH + 1)]  # uses the embedded id
    assert graph.has_edge(ITEM_A, OWNER_ID)  # ...and so does the edge


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
    queue = asyncio.Queue()
    graph = nx.DiGraph()

    await PageHandler(client, FakeDispatcher(queue), graph).handle(input_job(direction))

    assert graph.has_edge(*edge)
    assert graph.number_of_edges() == 1
    # The member is a BARE endpoint node — ActorHandler enriches it later.
    assert dict(graph.nodes[ITEM_A]) == {}


# ---------------------------------------------------------------------------
# next -> page job
# ---------------------------------------------------------------------------


async def test_enqueues_next_as_a_page_job():
    client = FakeActivityPubClient(doc=page_doc([ITEM_A], next_id=NEXT_ID))
    queue = asyncio.Queue()

    await PageHandler(client, FakeDispatcher(queue), nx.DiGraph()).handle(input_job())

    page_jobs = [j for j in drain(queue) if j["job_type"] == "page"]
    # Same owner/direction/depth — it's more of the same collection.
    assert page_jobs == [next_page_job(NEXT_ID)]


async def test_no_next_means_no_page_job():
    client = FakeActivityPubClient(doc=page_doc([ITEM_A]))  # no next
    queue = asyncio.Queue()

    await PageHandler(client, FakeDispatcher(queue), nx.DiGraph()).handle(input_job())

    page_jobs = [j for j in drain(queue) if j["job_type"] == "page"]
    assert page_jobs == []


def test_next_available_delegates_to_the_client_for_the_page_url():
    client = FakeActivityPubClient()
    handler = PageHandler(client, FakeDispatcher(asyncio.Queue()), nx.DiGraph())

    result = handler.next_available(input_job())

    # It HANDLES page jobs, so it asks its client about the page URL it'll fetch.
    assert result == NA_RESULT
    assert client.na_calls == [PAGE_ID]
