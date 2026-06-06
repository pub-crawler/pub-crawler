"""Tests for main.crawl_graph (step 2) — seeds file -> nodes-only GML.

Black-box: crawl_graph reads webfinger addresses (one per line), resolves each
to an actor id, builds a graph of those nodes, and writes it to the output path
(created or replaced). The queue/runner are internal. A single httpx.MockTransport
serves the webfinger lookups; the graph is read back with networkx to assert.

Assumed contract:
  async crawl_graph(input_filename, output_filename, *, transport=None, redis=None)
    -> writes a GML graph whose nodes are the resolved actor ids
  Blank/whitespace lines are ignored; a seed that fails to resolve is skipped
  (the run continues); the output file is created or replaced.
"""

import httpx
import networkx as nx
from fakeredis import FakeAsyncRedis, FakeServer

from main import crawl_graph

EVAN = "https://cosocial.ca/users/evan"
ALICE = "https://example.social/users/alice"


def fake_redis():
    # Fresh, isolated in-memory async Redis (its own server) per call.
    return FakeAsyncRedis(server=FakeServer())


def webfinger_handler(failing=()):
    """Resolve acct:user@host -> https://host/users/user; 404 for `failing` accts."""

    def handler(request):
        assert request.url.path == "/.well-known/webfinger"
        resource = request.url.params["resource"]  # acct:user@host
        acct = resource.removeprefix("acct:")
        if acct in failing:
            return httpx.Response(404, json={})
        user, host = acct.split("@")
        actor = f"https://{host}/users/{user}"
        return httpx.Response(
            200,
            json={
                "subject": resource,
                "links": [
                    {"rel": "self", "type": "application/activity+json", "href": actor}
                ],
            },
        )

    return handler


async def test_writes_resolved_nodes_to_gml(tmp_path):
    seeds = tmp_path / "seeds.txt"
    seeds.write_text("evan@cosocial.ca\nalice@example.social\n")
    out = tmp_path / "graph.gml"

    await crawl_graph(
        str(seeds),
        str(out),
        transport=httpx.MockTransport(webfinger_handler()),
        redis=fake_redis(),
    )

    graph = nx.read_gml(str(out))
    assert set(graph.nodes) == {EVAN, ALICE}


async def test_ignores_blank_and_whitespace_lines(tmp_path):
    seeds = tmp_path / "seeds.txt"
    seeds.write_text("  evan@cosocial.ca  \n\n\nalice@example.social\n")
    out = tmp_path / "graph.gml"

    await crawl_graph(
        str(seeds),
        str(out),
        transport=httpx.MockTransport(webfinger_handler()),
        redis=fake_redis(),
    )

    graph = nx.read_gml(str(out))
    assert set(graph.nodes) == {EVAN, ALICE}


async def test_a_failing_seed_is_skipped_not_fatal(tmp_path):
    seeds = tmp_path / "seeds.txt"
    seeds.write_text("evan@cosocial.ca\nnobody@example.social\n")
    out = tmp_path / "graph.gml"
    handler = webfinger_handler(failing={"nobody@example.social"})

    await crawl_graph(
        str(seeds),
        str(out),
        transport=httpx.MockTransport(handler),
        redis=fake_redis(),
    )

    graph = nx.read_gml(str(out))
    assert set(graph.nodes) == {EVAN}


async def test_replaces_an_existing_output_file(tmp_path):
    out = tmp_path / "graph.gml"
    out.write_text("not valid gml at all")
    seeds = tmp_path / "seeds.txt"
    seeds.write_text("evan@cosocial.ca\n")

    await crawl_graph(
        str(seeds),
        str(out),
        transport=httpx.MockTransport(webfinger_handler()),
        redis=fake_redis(),
    )

    graph = nx.read_gml(str(out))
    assert set(graph.nodes) == {EVAN}
