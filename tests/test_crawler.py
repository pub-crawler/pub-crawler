"""Integration test for crawler — seed a queue, drain it, check the graph.

Drives the REAL wiring: make_dispatcher() builds the dispatcher with all four
handlers; crawl_graph() runs the worker pool until the queue drains. A
MockTransport serves a shallow crawl (webfinger -> actor -> count-only
collections), a FakeAsyncRedis backs the queue, and a FakeGraph receives the
handlers' writes — so we assert the graph the crawl actually built. (GML output
is test_snapshot's job, not this one.)

Assumed contract (flag if different):
  make_dispatcher(redis, graph, *, transport=None, private_key_pem_data) -> Dispatcher
      (counters, clients, all four handlers registered — the test passes a
       redis-y thing, a graph-y thing, a transport-y thing, and the signing key
       contents; the rest is crawler.py's to build)
  async crawl_graph(dispatcher)
      creates the worker pool, drains the queue (join), winds the workers down
"""

import httpx
from fakeredis import FakeAsyncRedis, FakeServer

from crawler import crawl_graph, make_dispatcher
from support import FakeGraph

ACCT = "evan@cosocial.ca"
ACTOR_ID = "https://cosocial.ca/users/evan"
FOLLOWERS_URL = "https://cosocial.ca/users/evan/followers"
FOLLOWING_URL = "https://cosocial.ca/users/evan/following"
FOLLOWERS_TOTAL = 1200
FOLLOWING_TOTAL = 34

ACTOR_DOC = {
    "id": ACTOR_ID,
    "type": "Person",
    "preferredUsername": "evan",
    "name": "Evan Prodromou",
    "followers": FOLLOWERS_URL,
    "following": FOLLOWING_URL,
}


def fake_redis():
    return FakeAsyncRedis(server=FakeServer())


def crawl_handler(request):
    """A shallow fediverse: resolve the acct, serve the actor, count-only collections."""
    if request.url.path == "/.well-known/webfinger":
        resource = request.url.params["resource"]  # acct:evan@cosocial.ca
        return httpx.Response(
            200,
            json={
                "subject": resource,
                "links": [
                    {
                        "rel": "self",
                        "type": "application/activity+json",
                        "href": ACTOR_ID,
                    }
                ],
            },
        )
    url = str(request.url)
    if url == ACTOR_ID:
        return httpx.Response(200, json=ACTOR_DOC)
    if url == FOLLOWERS_URL:
        return httpx.Response(
            200,
            json={
                "id": FOLLOWERS_URL,
                "type": "OrderedCollection",
                "totalItems": FOLLOWERS_TOTAL,
            },
        )
    if url == FOLLOWING_URL:
        return httpx.Response(
            200,
            json={
                "id": FOLLOWING_URL,
                "type": "OrderedCollection",
                "totalItems": FOLLOWING_TOTAL,
            },
        )
    return httpx.Response(404, json={})


async def test_crawl_builds_the_graph_from_a_seed(keypair):
    pem, _ = keypair
    graph = FakeGraph()
    dispatcher = make_dispatcher(
        fake_redis(),
        graph,
        transport=httpx.MockTransport(crawl_handler),
        private_key_pem_data=pem,
    )

    # Seed one webfinger and let the crawl run to completion.
    await dispatcher.enqueue({"job_type": "webfinger", "webfinger": ACCT})
    await crawl_graph(dispatcher)

    # webfinger -> bare node; actor fetch -> stamped metadata; collections -> counts.
    assert await graph.has_node(ACTOR_ID)
    assert await graph.get_node_property(ACTOR_ID, "type") == "Person"
    assert await graph.get_node_property(ACTOR_ID, "preferred_username") == "evan"
    assert await graph.get_node_property(ACTOR_ID, "last_fetch_date") is not None
    assert await graph.get_node_property(ACTOR_ID, "followers_count") == FOLLOWERS_TOTAL
    assert await graph.get_node_property(ACTOR_ID, "following_count") == FOLLOWING_TOTAL
