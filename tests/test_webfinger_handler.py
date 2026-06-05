"""Tests for WebfingerHandler (step 4) — resolve a seed acct, enqueue its actor.

Step-4 contract: handle resolves the webfinger address to an actor id, adds it
to the graph as a bare node (no attrs — ActorHandler stamps those later), and
enqueues an actor job at depth 0. Jobs are packed dicts on one asyncio.Queue.

Pure unit tests via DI: a fake async webfinger client, a real asyncio.Queue, a
real networkx DiGraph. No HTTP.
"""

import asyncio

import networkx as nx
import pytest

from pub_crawler.webfinger_handler import WebfingerHandler

ACCT = "evan@cosocial.ca"
ACTOR_ID = "https://cosocial.ca/users/evan"

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


async def test_adds_the_actor_id_as_a_bare_node():
    client = FakeWebfingerClient()
    queue = asyncio.Queue()
    graph = nx.DiGraph()

    await WebfingerHandler(client, queue, graph).handle(WF_JOB)

    assert client.calls == [ACCT]
    assert graph.has_node(ACTOR_ID)
    # Still bare — ActorHandler fills in the metadata when it fetches.
    assert dict(graph.nodes[ACTOR_ID]) == {}


async def test_enqueues_the_actor_at_depth_zero():
    client = FakeWebfingerClient()
    queue = asyncio.Queue()
    graph = nx.DiGraph()

    await WebfingerHandler(client, queue, graph).handle(WF_JOB)

    assert queue.get_nowait() == ACTOR_JOB
    assert queue.empty()


async def test_lookup_failure_adds_nothing():
    client = FakeWebfingerClient(error=ValueError("no actor link"))
    queue = asyncio.Queue()
    graph = nx.DiGraph()

    with pytest.raises(ValueError):
        await WebfingerHandler(client, queue, graph).handle(WF_JOB)

    assert len(graph) == 0
    assert queue.empty()


def test_next_available_delegates_to_the_client_for_the_webfinger():
    client = FakeWebfingerClient()
    handler = WebfingerHandler(client, asyncio.Queue(), nx.DiGraph())

    result = handler.next_available(WF_JOB)

    # It HANDLES webfinger jobs, so it asks its client about the acct it'll resolve.
    assert result == NA_RESULT
    assert client.na_calls == [ACCT]
