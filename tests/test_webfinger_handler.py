"""Tests for WebfingerHandler (step 1) — resolve a seed acct, add a bare node.

Step-1 contract: handle resolves the webfinger address to an actor id and adds
it to the graph as a node with the actor id ONLY — no attributes (no
last_fetch_date; it hasn't been fetched, ActorHandler stamps that later). It
does NOT enqueue yet (that arrives in step 4).

Pure unit tests via DI: a fake async webfinger client, a spy queue that would
record any enqueue, and a real networkx DiGraph. No HTTP.
"""

import networkx as nx
import pytest

from pub_crawler.webfinger_handler import WebfingerHandler

ACCT = "evan@cosocial.ca"
ACTOR_ID = "https://cosocial.ca/users/evan"

# Handlers receive the packed job dict (matches main.py's queue payload).
WF_JOB = {"job_type": "webfinger", "webfinger": ACCT}


class FakeWebfingerClient:
    def __init__(self, result=ACTOR_ID, error=None):
        self.result = result
        self.error = error
        self.calls = []

    async def get_actor_id(self, wf):
        self.calls.append(wf)
        if self.error is not None:
            raise self.error
        return self.result


class SpyQueue:
    def __init__(self):
        self.actors = []  # (actor_id, depth) tuples, if anything were enqueued

    def add_actor(self, actor_id, depth):
        self.actors.append((actor_id, depth))


async def test_adds_the_actor_id_as_a_bare_node():
    client = FakeWebfingerClient()
    queue = SpyQueue()
    graph = nx.DiGraph()

    await WebfingerHandler(client, queue, graph).handle(WF_JOB)

    assert client.calls == [ACCT]
    assert graph.has_node(ACTOR_ID)
    # The actor id only — no attributes yet (not fetched).
    assert dict(graph.nodes[ACTOR_ID]) == {}


async def test_does_not_enqueue():
    client = FakeWebfingerClient()
    queue = SpyQueue()
    graph = nx.DiGraph()

    await WebfingerHandler(client, queue, graph).handle(WF_JOB)

    assert queue.actors == []


async def test_lookup_failure_adds_nothing():
    client = FakeWebfingerClient(error=ValueError("no actor link"))
    queue = SpyQueue()
    graph = nx.DiGraph()

    with pytest.raises(ValueError):
        await WebfingerHandler(client, queue, graph).handle(WF_JOB)

    assert len(graph) == 0
    assert queue.actors == []
