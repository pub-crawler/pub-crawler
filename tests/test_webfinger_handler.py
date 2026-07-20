"""Tests for WebfingerHandler (step 4) — resolve a seed acct, enqueue its actor.

Step-4 contract: handle resolves the webfinger address to an actor id, adds it
to the graph stamped with its seed depth (0, set-if-absent — a node already
discovered deeper keeps its existing depth), and enqueues an actor job at
depth 0. ActorHandler stamps the rest of the metadata later.

Pure unit tests via DI: a fake async webfinger client, a recording
FakeDispatcher (jobs land in a list), a FakeGraph. No HTTP.
"""

import pytest

from pub_crawler.webfinger_handler import WebfingerHandler
from support import FakeDispatcher, FakeGraph

ACCT = "evan@cosocial.example"
ACTOR_ID = "https://cosocial.example/users/evan"

WF_JOB = {"job_type": "webfinger", "webfinger": ACCT}
ACTOR_JOB = {"job_type": "actor", "actor_id": ACTOR_ID, "depth": 0}
NA_RESULT = 4242


class FakeWebfingerClient:
    def __init__(self, result=ACTOR_ID, error=None):
        self.result = result
        self.error = error
        self.calls = []
        self.na_calls = []

    async def get_actor_id(self, wf):
        self.calls.append(wf)
        if self.error is not None:
            raise self.error
        return self.result

    def next_available(self, wf):
        self.na_calls.append(wf)
        return NA_RESULT


async def test_adds_the_actor_id_stamped_with_seed_depth():
    client = FakeWebfingerClient()
    graph = FakeGraph()

    await WebfingerHandler(client, FakeDispatcher(), graph).handle(WF_JOB)

    assert client.calls == [ACCT]
    assert await graph.has_node(ACTOR_ID)
    # Only depth (0) — ActorHandler fills in the rest when it fetches.
    assert await graph.get_node_properties(ACTOR_ID) == {"depth": 0}


async def test_existing_depth_is_not_overwritten():
    # A seed discovered via someone's page BEFORE its webfinger ran keeps that
    # first-discovery depth; resolving the seed does not force it back to 0.
    client = FakeWebfingerClient()
    graph = FakeGraph()
    await graph.ensure_node(ACTOR_ID)
    await graph.set_node_property(ACTOR_ID, "depth", 2)

    await WebfingerHandler(client, FakeDispatcher(), graph).handle(WF_JOB)

    assert await graph.get_node_property(ACTOR_ID, "depth") == 2


async def test_enqueues_the_actor_at_depth_zero():
    client = FakeWebfingerClient()
    dis = FakeDispatcher()
    graph = FakeGraph()

    await WebfingerHandler(client, dis, graph).handle(WF_JOB)

    assert dis.enqueued == [ACTOR_JOB]


async def test_enqueues_an_existing_but_unfetched_actor():
    # The actor was already added as a bare node (e.g. discovered on some page)
    # but never fetched, so it has no last_fetch_date. A seed webfinger resolving
    # to it must STILL enqueue it — the enqueue is gated on last_fetch_date, not
    # on whether this handler created the node.
    client = FakeWebfingerClient()
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(ACTOR_ID)  # exists, but bare / unfetched

    await WebfingerHandler(client, dis, graph).handle(WF_JOB)

    assert dis.enqueued == [ACTOR_JOB]


async def test_does_not_enqueue_an_already_crawled_actor():
    # The seed resolves to an actor that's already been fetched (e.g. a re-run).
    # Don't re-enqueue it — the handle-time skip would just drop it anyway.
    client = FakeWebfingerClient()
    dis = FakeDispatcher()
    graph = FakeGraph()
    await graph.ensure_node(ACTOR_ID)
    await graph.set_node_property(ACTOR_ID, "last_fetch_date", "2026-06-01T00:00:00")

    await WebfingerHandler(client, dis, graph).handle(WF_JOB)

    assert dis.enqueued == []


async def test_lookup_failure_adds_nothing():
    client = FakeWebfingerClient(error=ValueError("no actor link"))
    dis = FakeDispatcher()
    graph = FakeGraph()

    with pytest.raises(ValueError):
        await WebfingerHandler(client, dis, graph).handle(WF_JOB)

    assert not await graph.has_node(ACTOR_ID)
    assert dis.enqueued == []


def test_next_available_delegates_to_the_client_for_the_webfinger():
    client = FakeWebfingerClient()
    handler = WebfingerHandler(client, FakeDispatcher(), FakeGraph())

    result = handler.next_available(WF_JOB)

    # It HANDLES webfinger jobs, so it asks its client about the acct it'll resolve.
    assert result == NA_RESULT
    assert client.na_calls == [ACCT]
