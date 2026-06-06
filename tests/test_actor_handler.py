"""Tests for ActorHandler — fetch an actor, stamp the node, enqueue collections.

handle fetches the actor via the signed client, adds/enriches the graph node
with scalar metadata + a last_fetch_date stamp, and enqueues a `collection` job
for each of followers/following (carrying the actor's depth). It enqueues
collections UNCONDITIONALLY — every node, including leaves, gets its counts; the
depth bound that stops page-walking lives in CollectionHandler now, not here.
Dedup: a node already stamped with last_fetch_date is skipped (no fetch/enqueue).

Pure DI unit tests: a fake async client, a recording FakeDispatcher, a DiGraph.

Assumed contract (flag if different):
  ActorHandler(client, dispatcher, graph).handle(job)
    job = {job_type:'actor', actor_id, depth}
    if node has last_fetch_date -> return (skip)
    actor = await client.get(actor_id); stamp node with scalar metadata + last_fetch_date
    enqueue a collection job for followers and following:
      {job_type:'collection', collection_id:<url>, owner_id:actor_id,
       direction:'followers'|'following', depth:<actor's depth>}
"""

import networkx as nx
import pytest

from pub_crawler.actor_handler import ActorHandler
from support import FakeDispatcher

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


class FakeActivityPubClient:
    def __init__(self, actor=ACTOR, error=None):
        self.actor = actor
        self.error = error
        self.calls = []
        self.na_calls = []

    async def get(self, url):
        self.calls.append(url)
        if self.error is not None:
            raise self.error
        return self.actor

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
    graph = nx.DiGraph()

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 0))

    assert client.calls == [ACTOR_ID]
    node = graph.nodes[ACTOR_ID]
    assert node["type"] == "Person"
    assert node["preferredUsername"] == "evan"
    assert isinstance(node["last_fetch_date"], str)
    assert node["last_fetch_date"]


async def test_enriches_an_existing_bare_node():
    client = FakeActivityPubClient()
    graph = nx.DiGraph()
    graph.add_node(ACTOR_ID)  # bare node from WebfingerHandler / PageHandler

    await make_handler(client, graph, FakeDispatcher()).handle(actor_job(ACTOR_ID, 1))

    assert client.calls == [ACTOR_ID]
    assert graph.nodes[ACTOR_ID]["type"] == "Person"
    assert "last_fetch_date" in graph.nodes[ACTOR_ID]


async def test_skips_an_already_fetched_node():
    client = FakeActivityPubClient()
    graph = nx.DiGraph()
    graph.add_node(ACTOR_ID, type="Person", last_fetch_date="2026-06-01T00:00:00")
    dis = FakeDispatcher()

    await make_handler(client, graph, dis).handle(actor_job(ACTOR_ID, 0))

    # Already stamped -> no re-fetch, no enqueue.
    assert client.calls == []
    assert dis.enqueued == []


async def test_fetch_failure_propagates_and_leaves_node_unstamped():
    client = FakeActivityPubClient(error=RuntimeError("boom"))
    graph = nx.DiGraph()
    graph.add_node(ACTOR_ID)
    dis = FakeDispatcher()

    with pytest.raises(RuntimeError):
        await make_handler(client, graph, dis).handle(actor_job(ACTOR_ID, 0))

    assert "last_fetch_date" not in graph.nodes[ACTOR_ID]
    assert dis.enqueued == []


# ---------------------------------------------------------------------------
# Enqueue collections — unconditional (leaves get counts too)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("depth", [0, 5])
async def test_enqueues_both_collections_carrying_the_actor_depth(depth):
    client = FakeActivityPubClient()
    graph = nx.DiGraph()
    dis = FakeDispatcher()

    await make_handler(client, graph, dis).handle(actor_job(ACTOR_ID, depth))

    jobs = dis.enqueued
    assert len(jobs) == 2
    # No depth gate here — collections go out at any depth, carrying the actor's.
    assert collection_job(FOLLOWERS_URL, "followers", depth) in jobs
    assert collection_job(FOLLOWING_URL, "following", depth) in jobs


def test_next_available_delegates_to_the_client_for_the_actor_url():
    client = FakeActivityPubClient()
    handler = make_handler(client, nx.DiGraph(), FakeDispatcher())

    result = handler.next_available(actor_job(ACTOR_ID, 0))

    # It HANDLES actor jobs, so it asks its client about the actor URL it'll fetch.
    assert result == NA_RESULT
    assert client.na_calls == [ACTOR_ID]
