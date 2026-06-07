"""Contract tests for the Graph interface — every implementation must satisfy them.

The same assertions run against each backend through the `graph` fixture:
FakeGraph now, DatabaseGraph (testcontainers Postgres) later. That's what keeps
FakeGraph provably equivalent to the real one — the handler tests trust the fake
only because these hold for both.

The interface is async (DatabaseGraph does async DB I/O):
  await ensure_node(label) / has_node(label)->bool / delete_node(label)
  await ensure_edge(f, t)   / has_edge(f, t)->bool / delete_edge(f, t)
  await set_node_property(label, name, value)
  await get_node_property(label, name)        -> value (None if absent)
  await get_node_properties(label)            -> {name: value}
  (edge equivalents)
  async for label, props in all_nodes()
  async for f, t, props in all_edges()

Assumptions flagged for confirmation:
  - all_nodes()/all_edges() yield (label, props) / (from, to, props) bundles
    (so the export is one pass, not N+1). Flip if you'd rather they yield bare
    ids and the export calls get_*_properties per item.
  - get_*_property returns None for an absent property.
  - delete_node's behaviour when edges still reference it (cascade vs. error) is
    NOT pinned here — that's the FK ON DELETE policy, your call.
"""

import pytest

from support import FakeGraph

A = "https://a.example/users/a"
B = "https://b.example/users/b"
C = "https://c.example/users/c"


@pytest.fixture
def graph():
    # Shared contract fixture; DatabaseGraph (testcontainers) joins as a param later.
    return FakeGraph()


# ---------------------------------------------------------------------------
# nodes: ensure / has / delete
# ---------------------------------------------------------------------------


async def test_ensure_node_then_has_node(graph):
    assert not await graph.has_node(A)
    await graph.ensure_node(A)
    assert await graph.has_node(A)


async def test_ensure_node_is_idempotent(graph):
    await graph.ensure_node(A)
    await graph.ensure_node(A)  # no error, still one node
    assert await graph.has_node(A)


async def test_delete_node(graph):
    await graph.ensure_node(A)
    await graph.delete_node(A)
    assert not await graph.has_node(A)


# ---------------------------------------------------------------------------
# edges: ensure / has / delete (directed)
# ---------------------------------------------------------------------------


async def test_ensure_edge_then_has_edge(graph):
    await graph.ensure_node(A)
    await graph.ensure_node(B)
    assert not await graph.has_edge(A, B)
    await graph.ensure_edge(A, B)
    assert await graph.has_edge(A, B)
    assert not await graph.has_edge(B, A)  # directed


async def test_ensure_edge_is_idempotent(graph):
    await graph.ensure_node(A)
    await graph.ensure_node(B)
    await graph.ensure_edge(A, B)
    await graph.ensure_edge(A, B)
    assert await graph.has_edge(A, B)


async def test_delete_edge(graph):
    await graph.ensure_node(A)
    await graph.ensure_node(B)
    await graph.ensure_edge(A, B)
    await graph.delete_edge(A, B)
    assert not await graph.has_edge(A, B)


# ---------------------------------------------------------------------------
# node properties (jsonb: types survive the round-trip)
# ---------------------------------------------------------------------------


async def test_set_and_get_node_property_keeps_type(graph):
    await graph.ensure_node(A)
    await graph.set_node_property(A, "followers_count", 42)
    await graph.set_node_property(A, "preferredUsername", "alice")
    assert await graph.get_node_property(A, "followers_count") == 42  # int, not "42"
    assert await graph.get_node_property(A, "preferredUsername") == "alice"


async def test_get_node_property_absent_is_none(graph):
    await graph.ensure_node(A)
    assert await graph.get_node_property(A, "missing") is None


async def test_set_node_property_overwrites(graph):
    await graph.ensure_node(A)
    await graph.set_node_property(A, "followers_count", 1)
    await graph.set_node_property(A, "followers_count", 99)
    assert await graph.get_node_property(A, "followers_count") == 99


async def test_get_node_properties_returns_all(graph):
    await graph.ensure_node(A)
    await graph.set_node_property(A, "followers_count", 5)
    await graph.set_node_property(A, "type", "Person")
    assert await graph.get_node_properties(A) == {
        "followers_count": 5,
        "type": "Person",
    }


# ---------------------------------------------------------------------------
# edge properties
# ---------------------------------------------------------------------------


async def test_set_and_get_edge_property(graph):
    await graph.ensure_node(A)
    await graph.ensure_node(B)
    await graph.ensure_edge(A, B)
    await graph.set_edge_property(A, B, "from_followers", True)
    assert await graph.get_edge_property(A, B, "from_followers") is True
    assert await graph.get_edge_properties(A, B) == {"from_followers": True}


# ---------------------------------------------------------------------------
# iteration (for the snapshot/GML export)
# ---------------------------------------------------------------------------


async def test_all_nodes_yields_label_and_props(graph):
    await graph.ensure_node(A)
    await graph.set_node_property(A, "followers_count", 5)
    await graph.ensure_node(B)  # no props

    seen = {label: props async for label, props in graph.all_nodes()}
    assert seen == {A: {"followers_count": 5}, B: {}}


async def test_all_edges_yields_endpoints_and_props(graph):
    for n in (A, B, C):
        await graph.ensure_node(n)
    await graph.ensure_edge(A, B)
    await graph.ensure_edge(A, C)  # no props
    await graph.set_edge_property(A, B, "direction", "followers")

    seen = {(f, t): props async for f, t, props in graph.all_edges()}
    assert seen == {(A, B): {"direction": "followers"}, (A, C): {}}
